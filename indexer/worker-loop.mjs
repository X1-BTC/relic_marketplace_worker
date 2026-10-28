
import {setTimeout as delay} from 'node:timers/promises';
import {PostgresMarketplaceWriter} from './postgres-writer.mjs';
import {indexOnce} from './run-once.mjs';
import {openMarketplaceLogSubscription} from './marketplace-ws.mjs';
import {acquireWorkerSession} from './worker-session.mjs';
import {WorkerFault} from './worker-config.mjs';

const jitter=ms=>Math.round(ms*(1+Math.random()*0.15));
async function sleep(ms,signal){if(ms<=0)return;try{await delay(ms,undefined,{signal});}catch{if(!signal.aborted)throw new WorkerFault('TIMER_FAILED');}}
const reorgMessage=e=>['Reorg: explicit recovery required','Reorg: recovery required','Reorg during collection'].includes(e?.message);
const bytecodeMismatch=e=>/^Unauthorized (nft|printer|marketplace) bytecode$/.test(e?.message ?? '');
const safeFailures=new Map([
  ['Batch must contain 1–1000 blocks','BATCH_RANGE_INVALID'],['Unconfirmed batch','BATCH_NOT_CONFIRMED'],
  ['Missing/failed receipt','RECEIPT_VALIDATION_FAILED'],['Log/receipt mismatch','RECEIPT_LOG_MISMATCH'],
  ['Reorg: explicit recovery required','REORG_ANCHOR_MISMATCH'],['Reorg: recovery required','REORG_ANCHOR_MISMATCH'],
  ['Reorg during collection','REORG_DURING_COLLECTION'],['Reorg below anchor: reviewed replacement required','REORG_RECOVERY_BELOW_ANCHOR'],
]);

const safeCodes=new Set(['WS_ENDPOINT_INVALID','WS_MARKETPLACE_ADDRESS_INVALID','WS_RUNTIME_UNAVAILABLE','WS_ABORTED','WS_CONNECT_FAILED','WS_SUBSCRIBE_SEND_FAILED','WS_SUBSCRIBE_REJECTED','WS_CLOSED','WS_CONNECT_TIMEOUT','WS_SOCKET_ERROR','WS_SOCKET_CLOSED','WS_WORKER_STOP','DATABASE_SESSION_LOST','DATABASE_SESSION_UNAVAILABLE','DATABASE_TLS_UNVERIFIED','DATABASE_IDENTITY_INVALID','DATABASE_OWNER_ROLE','DATABASE_QUERY_FAILED','DATABASE_CONNECT_FAILED','RPC_CHAIN_MISMATCH','CONFIG_INVALID','DEDICATED_ALCHEMY_HTTP_REQUIRED','DEDICATED_ALCHEMY_WS_REQUIRED','REORG_RECOVERY_FAILED']);
const workerCode=e=>safeCodes.has(e?.code)?e.code:safeFailures.get(e?.message)??'WORKER_CYCLE_FAILED';

// Diagnostic temporaire : masque les secrets courants avant journalisation.
function safeErrorMessage(error) {
  const message=error instanceof Error?error.message:String(error);
  return message
    .replace(/(?:https?|wss?):\/\/[^\s"'<>]+/gi,'[URL_REDACTED]')
    .replace(/0x[a-fA-F0-9]{40,}/g,'[HEX_REDACTED]')
    .replace(/(?:password|token|api[_-]?key|secret)\s*[:=]\s*[^\s,;]+/gi,'[SECRET_REDACTED]')
    .slice(0,500);
}

function wakeQueue() {
  let pending=false,removed=false,waiter=null;
  return {
    push({removed:wasRemoved=false}={}){pending=true;removed ||= wasRemoved;waiter?.();waiter=null;},
    take(timeoutMs,signal,closedPromise){
      if(pending){const result={kind:'notification',removed};pending=false;removed=false;return Promise.resolve(result);}
      return new Promise((resolve,reject)=>{
        let timer;
        const finish=value=>{clearTimeout(timer);signal?.removeEventListener('abort',aborted);waiter=null;resolve(value);};
        const aborted=()=>finish({kind:'aborted'});
        waiter=()=>{const wasRemoved=removed;pending=false;removed=false;clearTimeout(timer);signal?.removeEventListener('abort',aborted);waiter=null;resolve({kind:'notification',removed:wasRemoved});};
        timer=setTimeout(()=>finish({kind:'timeout'}),timeoutMs);
        signal?.addEventListener('abort',aborted,{once:true});
        closedPromise?.then(()=>finish({kind:'closed'}));
        if(signal?.aborted)aborted();
      });
    },
  };
}

function retryDelay(config,attempt){return jitter(Math.min(config.wsReconnectMaxMs,config.wsReconnectMinMs*2**Math.min(attempt-1,10)));}

async function runLeader({config,session,pool,makeClient,signal,log,openSubscription,metrics,writerFactory,onHealthy}) {
  const cycleSignal=AbortSignal.any([signal,session.signal]);
  const client=makeClient(cycleSignal);
  const writer=writerFactory({pool:session.pool,stream:config.stream,manifest:config.manifest});
  const wake=wakeQueue();
  let handle=null,wsConnected=false,disconnectedAt=Date.now(),disconnectAlerted=false;
  let reconnectFailures=0,nextReconnectAt=0,nextFallbackAt=0,lastCheckpoint=null,lastHead=null,checkpointStallAt=null,stallAlerted=false;

  const connect=async()=>{
    try{
      handle=await openSubscription({
        url:config.wsRpcUrl,address:config.manifest.marketplace,signal:cycleSignal,metrics,
        onLog:hint=>wake.push(hint),
        onState:state=>{
          if(state==='connected'){wsConnected=true;disconnectedAt=null;disconnectAlerted=false;}
          if(state==='disconnected'){
            const wasConnected=wsConnected;
            if(wsConnected){
              wsConnected=false;disconnectedAt??=Date.now();reconnectFailures++;
              const wait=retryDelay(config,reconnectFailures);nextReconnectAt=Date.now()+wait;
              log('ws_disconnected',{attempt:reconnectFailures,delayMs:wait});
              if(reconnectFailures>=3)log('alert',{code:'WS_RECONNECTS_REPEATED',attempt:reconnectFailures});
            }
            if(wasConnected)wake.push();
          }
        },
      });
      reconnectFailures=0;nextReconnectAt=0;
      log('ws_connected');
      return true;
    }catch(error){
      if(cycleSignal.aborted)return false;
      handle=null;wsConnected=false;disconnectedAt??=Date.now();reconnectFailures++;
      const wait=retryDelay(config,reconnectFailures);nextReconnectAt=Date.now()+wait;
      log('ws_reconnect_failed',{attempt:reconnectFailures,delayMs:wait,code:workerCode(error)});
      if(reconnectFailures>=3)log('alert',{code:'WS_RECONNECTS_REPEATED',attempt:reconnectFailures});
      return false;
    }
  };

  const maybeLogMetrics=()=>{
    const counts=metrics?.snapshot?.({reset:true});
    if(counts)log('rpc_metrics',counts);
  };

  const observeProgress=(cp,head)=>{
    const checkpoint=BigInt(cp.block_number),currentHead=BigInt(head);
    const lag=currentHead>BigInt(config.confirmations)+checkpoint?currentHead-BigInt(config.confirmations)-checkpoint:0n;
    const cpText=checkpoint.toString(),headText=currentHead.toString();
    if(lastCheckpoint!==cpText){lastCheckpoint=cpText;checkpointStallAt=null;stallAlerted=false;}
    if(lastHead!==null&&currentHead>BigInt(lastHead)&&lag>0n){checkpointStallAt??=Date.now();}
    if(lag>=BigInt(config.alertLagBlocks))log('alert',{code:'SYNC_LAG_HIGH',lagBlocks:lag.toString(),head:headText,checkpoint:cpText});
    if(checkpointStallAt&&Date.now()-checkpointStallAt>=config.alertStallMs&&!stallAlerted){
      stallAlerted=true;log('alert',{code:'CHECKPOINT_STALLED',lagBlocks:lag.toString(),head:headText,checkpoint:cpText,durationMs:Date.now()-checkpointStallAt});
    }
    lastHead=headText;
  };

  const catchUp=async({verifyIdle=false}={})=>{
    let cp=await writer.checkpoint();
    let head=await client.getBlockNumber();
    observeProgress(cp,head);
    let batches=0;
    while(!cycleSignal.aborted){
      let result;
      // The deployment checkpoint is B-1 (version 0), where Marketplace bytecode
      // is correctly absent. Keep canonical anchor/chain checks, but defer the
      // Marketplace runtime attestation until the first confirmed post-B batch.
      const attestIdle=verifyIdle===true&&BigInt(cp.version)>0n;
      try{result=await indexOnce({client,writer,confirmations:config.confirmations,batchSize:config.batchSize,checkpoint:cp,head,verifyIdle:attestIdle});}
      catch(error){
        if(!reorgMessage(error))throw error;
        const fresh=await writer.checkpoint();
        let recovered;
        try{recovered=await writer.recover({client,expectedVersion:fresh.version});}
        catch{log('alert',{code:'REORG_RECOVERY_FAILED'});throw new WorkerFault('REORG_RECOVERY_FAILED');}
        if(!recovered.unchanged)log('recovered',{checkpoint:String(recovered.blockNumber),version:String(recovered.version)});
        cp=await writer.checkpoint();head=await client.getBlockNumber();
        continue;
      }
      if(result.idle)break;
      batches++;
      cp={...cp,block_number:String(result.blockNumber),block_hash:result.blockHash,version:String(result.version)};
      head=await client.getBlockNumber();
      observeProgress(cp,head);
      const confirmed=head-BigInt(config.confirmations);
      const lag=confirmed>BigInt(cp.block_number)?confirmed-BigInt(cp.block_number):0n;
      log('progress',{head:head.toString(),checkpoint:String(cp.block_number),lagBlocks:lag.toString(),version:String(cp.version),batches});
      if(lag===0n)break;
      await sleep(config.catchupBatchDelayMs,cycleSignal);
    }
    const confirmed=head-BigInt(config.confirmations);
    const lag=confirmed>BigInt(cp.block_number)?confirmed-BigInt(cp.block_number):0n;
    if(lag===0n)log('idle',{head:head.toString(),checkpoint:String(cp.block_number),lagBlocks:'0',version:String(cp.version)});
    observeProgress(cp,head);
    maybeLogMetrics();
    return {checkpoint:cp,head,lag,batches};
  };

  // Subscribe first, then read from the durable checkpoint. Signals arriving
  // during catch-up are coalesced and cause another canonical HTTP pass.
  try{
    await connect();
    if(cycleSignal.aborted)return {aborted:true};
    nextFallbackAt=Date.now()+config.wsFallbackMs;
    await catchUp({verifyIdle:true});onHealthy?.();

    while(!cycleSignal.aborted){
      const now=Date.now();
      if(!wsConnected&&disconnectedAt&&now-disconnectedAt>=config.alertStallMs&&!disconnectAlerted){
        disconnectAlerted=true;log('alert',{code:'WS_DISCONNECTED_TOO_LONG',durationMs:now-disconnectedAt});
      }
      if(!wsConnected&&now>=nextReconnectAt){
        if(handle){try{await handle.close();}catch{}handle=null;}
        const connected=await connect();
        if(connected){nextFallbackAt=Date.now()+config.wsFallbackMs;await catchUp({verifyIdle:true});onHealthy?.();continue;}
      }
      if(now>=nextFallbackAt){
        await catchUp({verifyIdle:true});
        onHealthy?.();
        nextFallbackAt=Date.now()+config.wsFallbackMs;
        continue;
      }
      const reconnectDelayMs=!wsConnected?Math.max(1,nextReconnectAt-Date.now()):Infinity;
      const timeout=Math.max(1,Math.min(nextFallbackAt-Date.now(),reconnectDelayMs));
      const signalResult=await wake.take(timeout,cycleSignal,handle?.closed);
      if(signalResult.kind==='aborted')break;
      if(signalResult.kind==='closed'){
        try{await handle?.close();}catch{}
        handle=null;wsConnected=false;disconnectedAt??=Date.now();
        continue;
      }
      if(signalResult.kind==='timeout')continue;
      const caught=await catchUp({verifyIdle:signalResult.removed});
      onHealthy?.();
      if(signalResult.removed)log('reorg_signal',{checkpoint:String(caught.checkpoint.block_number)});
    }
    return {aborted:true};
  }finally{try{await handle?.close();}catch{}}
}

export async function runWorker({config,pool,makeClient,signal,log,onSession,openSubscription=openMarketplaceLogSubscription,metrics,writerFactory=options=>new PostgresMarketplaceWriter(options),onCycleError}) {
  let failures=0,standbyLogged=false;
  while(!signal.aborted){
    let session;
    try{
      session=await acquireWorkerSession(pool,{login:config.login,database:config.connection.database,signal});
      if(!session){
        if(!signal.aborted&&!standbyLogged){log('standby');standbyLogged=true;}
        await sleep(jitter(config.pollMs),signal);continue;
      }
      standbyLogged=false;onSession(session);log('leader',{serverVersion:session.version});
      await runLeader({config,session,pool,makeClient,signal,log,openSubscription,metrics,writerFactory,onHealthy:()=>{failures=0;}});
      if(!signal.aborted)throw new WorkerFault('DATABASE_SESSION_LOST');
    }catch(error){
      if(signal.aborted)break;
      try{onCycleError?.(error);}catch{}
      if(error instanceof WorkerFault&&error.fatal){log('fatal',{code:error.code});throw error;}
      if(bytecodeMismatch(error)){log('fatal',{code:'CONTRACT_BYTECODE_MISMATCH'});throw new WorkerFault('CONTRACT_BYTECODE_MISMATCH',{fatal:true});}
      failures++;
      const wait=jitter(Math.min(config.retryMaxMs,config.retryMinMs*2**Math.min(failures-1,10)));
      log('retry',{
        attempt:failures,
        delayMs:wait,
        code:workerCode(error),
        errorMessage:safeErrorMessage(error)
      });
      session?.close();onSession(null);
      await sleep(wait,signal);
    }finally{session?.close();onSession(null);}
  }
}
