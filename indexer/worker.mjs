import {readWorkerConfig,WorkerFault} from './worker-config.mjs';
import {createServerWriterPool} from './server-pool.mjs';
import {marketplaceOnlyClient} from './marketplace-only-client.mjs';
import {createCountingFetch,createRpcMetrics} from './rpc-metrics.mjs';
import {runWorker} from './worker-loop.mjs';
import {HyperSyncLogSource} from './hypersync-client.mjs';
import {envioHyperRpcUrl,hybridReadClient,createRpcStatusFetch} from './hybrid-rpc.mjs';
import {PRODUCTION_CONFIG} from '../shared/production-config.mjs';

// Closed log vocabulary: never pass error.message, stack, URL, env, manifest,
// SQL parameters or a driver object to stdout/stderr.
const events=new Set([
  'disabled','starting','leader','standby','idle','progress',
  'retry','recovered','fatal','stopping','stopped',
  'shutdown_deadline','ws_connected','ws_disconnected',
  'ws_reconnect_failed','reorg_signal','rpc_metrics','alert',
  'worker_status',
  // Ajouts pour les logs d’activité actuels
  'checkpoint_advanced','reconnect_scan','event_processed','event_pending_confirmation'
]);
const fields=new Set([
  'code','errorMessage','serverVersion','head','checkpoint',
  'lagBlocks','version','attempt','delayMs','durationMs','batches',
  'httpFetchCalls','wsConnectionAttempts','wsRequestsOut',
  'wsMessagesIn','wsNotifications','eth_blockNumber','eth_chainId',
  'eth_getBlockByNumber','eth_getTransactionReceipt',
  'eth_getCode','eth_call',
  'envioRpcCalls','envioRpcFailures','alchemyRpcCalls','alchemyEstimatedCu','alchemyUnpricedCalls',
  'hypersyncRequests','hypersyncPages','hypersyncFailures','hypersyncRangeFailures','hypersyncRetries','hypersyncBlocksVerified','hypersyncLogs','hypersyncVerifiedBlocksPerSecond',
  'heapUsedBytes','rssBytes','peakHeapUsedBytes','peakRssBytes',
  'state','component','reason',
  'httpStatus','jsonRpcCode','retryAfterMs','nextBlock','delayMs',
  // Ajouts pour les nouveaux événements
  'block','from','to'
]);
function log(event,data={}) {
  if(!events.has(event))return;
  const record={time:new Date().toISOString(),event};
  for(const [key,value] of Object.entries(data))if(fields.has(key))record[key]=value;
  process.stdout.write(JSON.stringify(record)+'\n');
}
let pool;let session;let deadline;let shutdownMs=90000;
const stop=new AbortController();
function shutdown(){
  if(stop.signal.aborted)return;
  log('stopping');stop.abort();
  deadline=setTimeout(()=>{log('shutdown_deadline');session?.close();process.exit(1);},shutdownMs);
  deadline.unref();
}
process.once('SIGTERM',shutdown);process.once('SIGINT',shutdown);
// Do not let Node print an unexpected driver/transport rejection containing an
// URL or connection object. A new process resumes from PostgreSQL, not memory.
function unexpectedFailure(){
  log('fatal',{code:'UNEXPECTED_RUNTIME_FAILURE'});stop.abort();
  try{session?.close();}catch{/* terminate below */}
  process.exit(1);
}
process.once('uncaughtException',unexpectedFailure);
process.once('unhandledRejection',unexpectedFailure);
try {
  const config=readWorkerConfig(process.env);
  if(!config.enabled){log('disabled');}
  else {
    shutdownMs=config.shutdownMs;
    if(PRODUCTION_CONFIG.transactionSubmissionEnabled!==false||PRODUCTION_CONFIG.marketplaceAddress!==null)throw new WorkerFault('FRONTEND_GATE_CHANGED',{fatal:true});
    let pg,viem;
    try{[pg,viem]=await Promise.all([import('pg'),import('viem')]);}catch{throw new WorkerFault('DEPENDENCY_UNAVAILABLE',{fatal:true});}
    // Refuse lossy global type parser changes before opening a connection.
    if(pg.types.getTypeParser(20)('9007199254740993')!=='9007199254740993'||pg.types.getTypeParser(1700)('100000000000000000025')!=='100000000000000000025')throw new WorkerFault('LOSSY_DATABASE_PARSER',{fatal:true});
    pool=createServerWriterPool({Pool:pg.Pool,connection:config.connection});
    const metrics=createRpcMetrics();
    const chain=viem.defineChain({id:4663,name:'Robinhood Mainnet',nativeCurrency:{name:'RHBTC',symbol:'RHBTC',decimals:18},rpcUrls:{default:{http:[config.httpRpcUrl]}}});
    const envioRpcUrl=envioHyperRpcUrl(config.envioApiToken);
    const makeClient=signal=>{
      const envioFetch=createRpcStatusFetch();
      const alchemy=marketplaceOnlyClient(viem.createPublicClient({
        chain,transport:viem.http(config.httpRpcUrl,{retryCount:0,timeout:15000,batch:false,fetchFn:createCountingFetch(metrics),fetchOptions:{signal}}),cacheTime:0,
      }),config.manifest,metrics);
      const envio=marketplaceOnlyClient(viem.createPublicClient({
        chain,transport:viem.http(envioRpcUrl,{retryCount:0,timeout:15000,batch:false,fetchFn:envioFetch,fetchOptions:{signal}}),cacheTime:0,
      }),config.manifest,metrics);
      return hybridReadClient({alchemy,envio,metrics,rpcFacts:envioFetch.facts,onCapability:(method,state,facts)=>{
        if(state==='unsupported')log('worker_status',{state:'ENVIO_RPC_UNSUPPORTED',component:method});
        else if(state==='unavailable')log('worker_status',{state:'ENVIO_RPC_UNAVAILABLE',component:method,httpStatus:facts?.httpStatus,jsonRpcCode:facts?.jsonRpcCode});
      }});
    };
    const makeLogSource=signal=>new HyperSyncLogSource({apiToken:config.envioApiToken,signal,metrics,
      onReady:()=>log('worker_status',{state:'ENVIO_READY',component:'ENVIO'}),
      onRetry:info=>log('worker_status',{state:'RETRYING',component:'ENVIO',reason:info.code,attempt:info.attempt,delayMs:info.delayMs,httpStatus:info.httpStatus,retryAfterMs:info.retryAfterMs})});
    log('starting');
    await runWorker({config,pool,makeClient,makeLogSource,signal:stop.signal,log,onSession:value=>{session=value;},metrics});
  }
}catch(error){
  if(!stop.signal.aborted){log('fatal',{code:error instanceof WorkerFault?error.code:'WORKER_FAILED'});process.exitCode=1;}
}finally{
  session?.close();
  try{await pool?.end();}catch{process.exitCode=1;}
  clearTimeout(deadline);log('stopped');
}
