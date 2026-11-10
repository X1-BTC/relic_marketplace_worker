import {pathToFileURL} from 'node:url';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {readWorkerConfig} from '../indexer/worker-config.mjs';
import {createServerWriterPool} from '../indexer/server-pool.mjs';
import {acquireWorkerSession} from '../indexer/worker-session.mjs';
import {PostgresMarketplaceWriter} from '../indexer/postgres-writer.mjs';
import {verifyChain} from '../indexer/marketplace.mjs';
import {PRODUCTION_CONFIG} from '../shared/production-config.mjs';

const execFileAsync=promisify(execFile);
const SALE_TX='0x43d9b857246f88429a9e24d1e1d4b9dd620e0b69bd0c61255cfca57b5298699a';
const SELLER='0x591f30b91573a072be38c0c6e42a04a48eab817b';
const BUYER='0x1ffee3db7d6135595e2d50af515b5f137751f5c9';
const PLATFORM='0x87cee0881b121f3557e4b319d35d75dedbfebe2b';
const THIRDWEB='0x1af20c6b23373350ad464700b5965ce4b0d2ad94';
const TOKEN=PRODUCTION_CONFIG.currencyAddress;
const AMOUNTS={gross:'9000000000000000000',seller:'8550000000000000000',platform:'405000000000000000',thirdweb:'45000000000000000'};
const emit=(event,fields={})=>process.stdout.write(`${JSON.stringify({time:new Date().toISOString(),event,...fields})}\n`);
const ensure=(ok,code)=>{if(!ok)throw Error(code);};
function expectedSale(summary){
  const check=summary.financialValidation?.sales?.find(x=>x.transactionHash===SALE_TX&&Number(x.logIndex)===4);
  ensure(check,'KNOWN_SALE_EVIDENCE_NOT_FOUND');
  const s=check.expected.settlement;
  ensure(s&&s.reconcile_status==='RECONCILED'&&s.listing_id==='2'&&s.nft_contract===PRODUCTION_CONFIG.nftAddress&&s.token_id==='1509'&&s.buyer_address===BUYER&&s.seller_address===SELLER&&s.currency_address===TOKEN&&s.currency_decimals==='18'&&s.gross_amount_raw===AMOUNTS.gross&&s.seller_proceeds_raw===AMOUNTS.seller&&s.platform_fee_raw===AMOUNTS.platform&&s.thirdweb_fee_raw===AMOUNTS.thirdweb,'KNOWN_SALE_EXPECTED_SETTLEMENT_MISMATCH');
  ensure(check.expected.payments.length===3,'KNOWN_SALE_PAYMENT_TRANSFER_COUNT_MISMATCH');
  const p=check.expected.payments;
  ensure(p.some(x=>x.from_address===BUYER&&x.to_address===SELLER&&x.amount_raw===AMOUNTS.seller&&x.payment_role==='SELLER'&&x.token_contract===TOKEN),'KNOWN_SALE_SELLER_TRANSFER_MISMATCH');
  ensure(p.some(x=>x.from_address===BUYER&&x.to_address===PLATFORM&&x.amount_raw===AMOUNTS.platform&&x.payment_role==='PLATFORM'&&x.token_contract===TOKEN),'KNOWN_SALE_PLATFORM_TRANSFER_MISMATCH');
  ensure(p.some(x=>x.from_address===BUYER&&x.to_address===THIRDWEB&&x.amount_raw===AMOUNTS.thirdweb&&x.payment_role==='THIRDWEB'&&x.token_contract===TOKEN),'KNOWN_SALE_THIRDWEB_TRANSFER_MISMATCH');
  ensure(check.expected.nft.length===1&&check.expected.nft[0].from_address===SELLER&&check.expected.nft[0].to_address===BUYER&&check.expected.nft[0].token_id==='1509','KNOWN_SALE_NFT_TRANSFER_MISMATCH');
  return check;
}
async function audit(){
  try{
    const {stdout}=await execFileAsync(process.execPath,['scripts/audit-marketplace-history.mjs','--targeted'],{cwd:process.cwd(),env:process.env,maxBuffer:16*1024*1024,timeout:180000});
    const summaries=stdout.trim().split('\n').map(line=>{try{return JSON.parse(line);}catch{return null;}}).filter(x=>x?.event==='targeted_audit_summary');
    ensure(summaries.length===1,'TARGETED_AUDIT_SUMMARY_MISSING');return summaries[0];
  }catch{throw Error('TARGETED_READ_ONLY_AUDIT_FAILED');}
}
function makeLeasedPool(Pool,connection){
  const raw=new Pool({...connection,application_name:'relic-market-historical-replay',max:1,connectionTimeoutMillis:15000,
    options:'-c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000'});
  raw.on('error',()=>{});
  return {raw,pool:{async connect(){const db=await raw.connect();try{await db.query('SET ROLE relic_market_writer');await db.query('SET search_path=pg_catalog');return db;}catch{db.release(true);throw Error('RECOVERY_DATABASE_SESSION_UNAVAILABLE');}},end:()=>raw.end()}};
}
async function main(){
  const apply=process.argv.includes('--apply');
  if(apply){ensure(process.env.RM_ALLOW_PRODUCTION_MARKETPLACE_REPLAY==='true','PRODUCTION_REPLAY_EXPLICIT_ENV_APPROVAL_REQUIRED');ensure(process.argv.includes('--maintenance-confirmation=STOPPED_AND_READY'),'MAINTENANCE_CONFIRMATION_REQUIRED');}
  const config=readWorkerConfig(process.env);ensure(config.enabled,'WORKER_DISABLED');
  if(!apply){
    const summary=await audit();
    expectedSale(summary);
    emit('historical_replay_dry_run',{checkpoint:summary.checkpoint,missingCount:summary.missingDatabaseIdentities.length,decodedTypes:summary.eventCountsByDecodedType,financialValidation:summary.financialValidation,transactionCount:summary.transactions.length,replayBatches:summary.replayBatches,writePerformed:false,commandForApply:'node scripts/replay-marketplace-history.mjs --apply --maintenance-confirmation=STOPPED_AND_READY --expected-checkpoint-version=<reviewed-version>'});
    return;
  }
  const versionArg=process.argv.find(x=>x.startsWith('--expected-checkpoint-version='))?.split('=')[1];
  ensure(versionArg&&/^(0|[1-9][0-9]*)$/.test(versionArg),'EXPECTED_CHECKPOINT_VERSION_REQUIRED');
  const pg=await import('pg'),viem=await import('viem');
  const {raw,pool}=makeLeasedPool(pg.Pool,config.connection);let session=null;
  try{
    session=await acquireWorkerSession(pool,{login:config.login,database:config.connection.database,signal:new AbortController().signal});
    ensure(session,'WORKER_ADVISORY_LOCK_BUSY');
    const summary=await audit();
    ensure(summary.checkpoint.version===versionArg,'CHECKPOINT_VERSION_CHANGED_SINCE_APPROVAL');
    ensure(summary.replayReady===true,'TARGETED_REPLAY_NOT_SAFE');
    ensure(summary.ambiguousSettlements.length===0,'AMBIGUOUS_SETTLEMENT_BLOCKS_REPLAY');
    ensure(summary.missingDatabaseIdentities.length>0,'NO_MISSING_IDENTITIES');
    expectedSale(summary);
    const chain=viem.defineChain({id:4663,name:'Robinhood Mainnet',nativeCurrency:{name:'RHBTC',symbol:'RHBTC',decimals:18},rpcUrls:{default:{http:[config.httpRpcUrl]}}});
    const client=viem.createPublicClient({chain,transport:viem.http(config.httpRpcUrl,{retryCount:0,timeout:15000,batch:false}),cacheTime:0});
    await verifyChain(client,config.manifest,Number(summary.checkpoint.blockNumber));
    ensure(await client.getChainId()===4663,'AUDIT_CHAIN_ID_MISMATCH');
    const writer=new PostgresMarketplaceWriter({pool:session.pool,stream:config.stream,manifest:config.manifest});
    const results=[];
    for(const batch of summary.replayBatches){
      ensure(String(batch.expectedVersion)===versionArg,'REPLAY_BATCH_VERSION_MISMATCH');
      const head=await client.getBlockNumber();ensure(BigInt(batch.range.toBlock)+12n<=head,'REPLAY_BATCH_CONFIRMATIONS_INSUFFICIENT');
      for(const b of batch.blocks){const canonical=await client.getBlock({blockNumber:BigInt(b.number)});ensure(canonical.hash.toLowerCase()===b.hash.toLowerCase()&&canonical.parentHash.toLowerCase()===b.parentHash.toLowerCase(),'REPLAY_BLOCK_REORGED');}
      results.push(await writer.replayHistorical(batch));
    }
    const after=await writer.checkpoint();
    ensure(String(after.version)===versionArg&&String(after.block_number)===summary.checkpoint.blockNumber,'REPLAY_CHANGED_CHECKPOINT');
    const verified=await audit();
    ensure(verified.missingDatabaseIdentities.length===0,'POST_REPLAY_IDENTITIES_INCOMPLETE');
    ensure(verified.financialValidation.allSalesMatch===true,'POST_REPLAY_FINANCIAL_PROJECTION_MISMATCH');
    const sale=expectedSale(verified);ensure(sale.matches,'POST_REPLAY_KNOWN_SALE_DATABASE_MISMATCH');
    emit('historical_replay_complete',{transactions:verified.transactions.length,eventCountsByDecodedType:verified.eventCountsByDecodedType,replayedBlocks:results.reduce((n,x)=>n+x.replayedBlocks,0),replayedEvents:results.reduce((n,x)=>n+x.replayedEvents,0),checkpoint:{blockNumber:String(after.block_number),blockHash:after.block_hash,version:String(after.version)},sale1509:sale,writePerformed:true});
  }finally{session?.close();await pool.end();}
}

export {expectedSale};
if(import.meta.url===pathToFileURL(process.argv[1]??'').href)main().catch(error=>{emit('historical_replay_failed',{code:/^[A-Z0-9_]+$/.test(error.message)?error.message:'HISTORICAL_REPLAY_FAILED'});process.exitCode=1;});
