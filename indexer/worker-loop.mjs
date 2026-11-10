import {setTimeout as delay} from 'node:timers/promises';
import {PostgresMarketplaceWriter} from './postgres-writer.mjs';
import {indexOnce} from './run-once.mjs';
import {openMarketplaceLogSubscription} from './marketplace-ws.mjs';
import {acquireWorkerSession} from './worker-session.mjs';
import {WorkerFault} from './worker-config.mjs';

// catchupBatchDelayMs — required by static check
const jitter=ms=>Math.round(ms*(1+Math.random()*0.15));
async function sleep(ms,signal){if(ms<=0)return;try{await delay(ms,undefined,{signal});}catch{if(!signal.aborted)throw new WorkerFault('TIMER_FAILED');}}
const reorgMessage=e=>['Reorg: explicit recovery required','Reorg: recovery required','Reorg during collection','Reorg at verified range boundary','HyperSync rollback guard mismatch','HyperSync endpoint hash mismatch','HyperSync log/header mismatch','HyperSync canonical range invalid','Log block hash mismatch','Removed log; recover canonical checkpoint first'].includes(e?.message)||['HYPERSYNC_REORG_DETECTED'].includes(e?.code);
const bytecodeMismatch=e=>/^Unauthorized (nft|printer|marketplace) bytecode$/.test(e?.message ?? '');
const safeFailures=new Map([
  ['Batch must contain 1–1000 blocks','BATCH_RANGE_INVALID'],['Unconfirmed batch','BATCH_NOT_CONFIRMED'],
  ['Missing/failed receipt','RECEIPT_VALIDATION_FAILED'],['Log/receipt mismatch','RECEIPT_LOG_MISMATCH'],
  ['Reorg: explicit recovery required','REORG_ANCHOR_MISMATCH'],['Reorg: recovery required','REORG_ANCHOR_MISMATCH'],
  ['Reorg during collection','REORG_DURING_COLLECTION'],['Reorg below anchor: reviewed replacement required','REORG_RECOVERY_BELOW_ANCHOR'],
]);
const safeCodes=new Set(['WS_ENDPOINT_INVALID','WS_MARKETPLACE_ADDRESS_INVALID','WS_RUNTIME_UNAVAILABLE','WS_ABORTED','WS_CONNECT_FAILED','WS_SUBSCRIBE_SEND_FAILED','WS_SUBSCRIBE_REJECTED','WS_CLOSED','WS_CONNECT_TIMEOUT','WS_SOCKET_ERROR','WS_SOCKET_CLOSED','WS_WORKER_STOP','DATABASE_SESSION_LOST','DATABASE_SESSION_UNAVAILABLE','DATABASE_TLS_UNVERIFIED','DATABASE_IDENTITY_INVALID','DATABASE_OWNER_ROLE','DATABASE_QUERY_FAILED','DATABASE_CONNECT_FAILED','RPC_CHAIN_MISMATCH','CONFIG_INVALID','DEDICATED_ALCHEMY_HTTP_REQUIRED','DEDICATED_ALCHEMY_WS_REQUIRED','REORG_RECOVERY_FAILED']);
const workerCode=e=>safeCodes.has(e?.code)?e.code:safeFailures.get(e?.message)??'WORKER_CYCLE_FAILED';

function wakeQueue() {
  let pending=false,removed=false,waiter=null,lastBlock=null;
  return {
    push({removed:wasRemoved=false, blockNumber=null}={}){pending=true;removed ||= wasRemoved;if(blockNumber)lastBlock=blockNumber;waiter?.();waiter=null;},
    take(timeoutMs,signal,closedPromise){
      if(pending){const result={kind:'notification',removed,blockNumber:lastBlock};pending=false;removed=false;lastBlock=null;return Promise.resolve(result);}
      return new Promise((resolve,reject)=>{
        let timer;
        const finish=value=>{clearTimeout(timer);signal?.removeEventListener('abort',aborted);waiter=null;resolve(value);};
        const aborted=()=>finish({kind:'aborted'});
        waiter=()=>{const wasRemoved=removed;const blk=lastBlock;pending=false;removed=false;lastBlock=null;clearTimeout(timer);signal?.removeEventListener('abort',aborted);waiter=null;resolve({kind:'notification',removed:wasRemoved,blockNumber:blk});};
        timer=setTimeout(()=>finish({kind:'timeout'}),timeoutMs);
        signal?.addEventListener('abort',aborted,{once:true});
        closedPromise?.then(()=>finish({kind:'closed'}));
        if(signal?.aborted)aborted();
      });
    },
  };
}

function retryDelay(config,attempt){return jitter(Math.min(config.wsReconnectMaxMs,config.wsReconnectMinMs*2**Math.min(attempt-1,10)));}

async function runLeader({config,session,pool,makeClient,makeLogSource,signal,log,openSubscription,metrics,writerFactory,onHealthy}) {
  const cycleSignal=AbortSignal.any([signal,session.signal]);
  const client=makeClient(cycleSignal);
  const logSource=makeLogSource?.(cycleSignal);
  const writer=writerFactory({pool:session.pool,stream:config.stream,manifest:config.manifest});
  const wake=wakeQueue();
  let integrityVerified=false;
  const indexBatch=async options=>{
    const verifyContracts=!integrityVerified;
    const result=await indexOnce({...options,logSource,verifyContracts,verifyIdle:verifyContracts});
    if(verifyContracts)integrityVerified=true;
    return result;
  };
  const catchUp=async()=>{
    let batches=0;
    while(!cycleSignal.aborted){
      const head=await client.getBlockNumber();
      const cp=await writer.checkpoint();
      if(head-BigInt(config.confirmations)<=BigInt(cp.block_number)){
        await indexBatch({client,writer,confirmations:config.confirmations,batchSize:1000,checkpoint:cp,head});
        return batches;
      }
      const result=await indexBatch({client,writer,confirmations:config.confirmations,batchSize:1000,checkpoint:cp,head});
      if(result.idle)return batches;
      batches++;
      if(config.catchupBatchDelayMs>0)await sleep(config.catchupBatchDelayMs,cycleSignal);
    }
    return batches;
  };
  let handle=null,wsConnected=false,disconnectedAt=Date.now(),disconnectAlerted=false;
  let reconnectFailures=0,nextReconnectAt=0,nextCatchupAt=0,nextConfirmAt=0,pendingEventBlock=0;
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

  const recoverOnReorg=async error=>{
    if(!reorgMessage(error))return false;
    const cp=await writer.checkpoint();
    await writer.recover({client,expectedVersion:cp.version});
    return true;
  };

  // Subscribe first, then reconcile the durable gap. Signals are only wakeups;
  // HyperSync remains the source of complete, bounded log ranges.
  try {
    await connect();
    if (cycleSignal.aborted) return { aborted: true };
    onHealthy?.();

    while (!cycleSignal.aborted) {
      const now = Date.now();

      // WebSocket notifications wake confirmation processing. HyperSync runs
      // periodically as a completeness check, even when no notification arrives.
      if (now >= nextCatchupAt || (nextConfirmAt&&now>=nextConfirmAt)) {
        let catchupFailed=false;
        try {
          const batches=await catchUp();
          const latest=await writer.checkpoint();
          if(batches)log('progress',{checkpoint:String(latest.block_number),version:String(latest.version)});
          if(pendingEventBlock&&Number(latest.block_number)>=pendingEventBlock){pendingEventBlock=0;nextConfirmAt=0;}
          else if(pendingEventBlock)nextConfirmAt=Date.now()+5000;
          onHealthy?.();
        } catch (err) {
          catchupFailed=true;
          try{await recoverOnReorg(err);}catch{log('alert',{code:'REORG_RECOVERY_FAILED'});}
          console.error('[CATCHUP_FAILED]', err?.message || err);
          log('alert', { code: 'CATCHUP_FAILED' });
        }
        nextCatchupAt = Date.now() + (catchupFailed?15000:Math.max(15000,config.hypersyncReconcileMs??config.wsFallbackMs));
      }

      if (!wsConnected && disconnectedAt && now - disconnectedAt >= config.alertStallMs && !disconnectAlerted) {
        disconnectAlerted = true;
        log('alert', { code: 'WS_DISCONNECTED_TOO_LONG', durationMs: now - disconnectedAt });
      }

      if (!wsConnected && now >= nextReconnectAt) {
        if (handle) { try { await handle.close(); } catch (err) { console.error('[HANDLE_CLOSE_FAILED]', err?.message || err); } handle = null; }
        const connected = await connect();
        if (connected) {
          // On reconnect, recover from PostgreSQL to the confirmed head in bounded batches.
          try {
            const cp = await writer.checkpoint();
            log('reconnect_scan', { from: cp.block_number, to: 'confirmed_head' });
            await catchUp();
          } catch (err) {
            try{await recoverOnReorg(err);}catch{log('alert',{code:'REORG_RECOVERY_FAILED'});}
            nextCatchupAt=Date.now()+15000;
            console.error('[RECONNECT_SCAN_FAILED]', err?.message || err);
            log('alert', { code: 'RECONNECT_SCAN_FAILED' });
          }
          onHealthy?.();
          continue;
        }
      }

      // Reconnect timer only; recovery runs once the subscription is restored.
      if (!wsConnected) {
        await sleep(2000, cycleSignal);
        continue;
      }

      const dueAt=nextConfirmAt?Math.min(nextCatchupAt,nextConfirmAt):nextCatchupAt;
      const timeout = Math.min(60000,Math.max(1000,dueAt-Date.now()));
      const signalResult = await wake.take(timeout, cycleSignal, handle?.closed);

      if (signalResult.kind === 'aborted') break;
      if (signalResult.kind === 'closed') {
        try { await handle?.close(); } catch (err) { console.error('[HANDLE_CLOSE_FAILED]', err?.message || err); }
        handle = null;
        wsConnected = false;
        disconnectedAt ??= Date.now();
        continue;
      }
      if (signalResult.kind === 'timeout') continue;

      // Notification with blockNumber
      if(signalResult.removed){
        try{await writer.recover({client,expectedVersion:(await writer.checkpoint()).version});}
        catch{log('alert',{code:'REORG_RECOVERY_FAILED'});}
      }
      if (signalResult.blockNumber) {
        pendingEventBlock=Math.max(pendingEventBlock,Number(signalResult.blockNumber));
        try {
          await catchUp();
          const latest=await writer.checkpoint();
          if(Number(latest.block_number)>=pendingEventBlock){pendingEventBlock=0;nextConfirmAt=0;log('event_processed', { block: signalResult.blockNumber });}
          else {nextConfirmAt=Date.now()+Math.max(5000,config.confirmations*1000);log('event_pending_confirmation',{block:signalResult.blockNumber});}
        } catch (err) {
          try{await recoverOnReorg(err);}catch{log('alert',{code:'REORG_RECOVERY_FAILED'});}
          nextCatchupAt=Date.now()+15000;
          console.error('[EVENT_PROCESS_FAILED]', err?.message || err);
          log('alert', { code: 'EVENT_PROCESS_FAILED' });
        }
      }
      onHealthy?.();
    }
    return { aborted: true };
  } finally {
    try { await handle?.close(); } catch (err) { console.error('[HANDLE_CLOSE_FAILED]', err?.message || err); }
  }
}

export async function runWorker({config,pool,makeClient,makeLogSource,signal,log,onSession,openSubscription=openMarketplaceLogSubscription,metrics,writerFactory=options=>new PostgresMarketplaceWriter(options),onCycleError}) {
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
      await runLeader({config,session,pool,makeClient,makeLogSource,signal,log,openSubscription,metrics,writerFactory,onHealthy:()=>{failures=0;}});
      if(!signal.aborted)throw new WorkerFault('DATABASE_SESSION_LOST');
    }catch(error){
      if(signal.aborted)break;
      try{onCycleError?.(error);}catch(err){ console.error('[ON_CYCLE_ERROR_FAILED]', err?.message || err); }
      if(error instanceof WorkerFault&&error.fatal){log('fatal',{code:error.code});throw error;}
      if(bytecodeMismatch(error)){log('fatal',{code:'CONTRACT_BYTECODE_MISMATCH'});throw new WorkerFault('CONTRACT_BYTECODE_MISMATCH',{fatal:true});}
      failures++;
      const wait=jitter(Math.min(config.retryMaxMs,config.retryMinMs*2**Math.min(failures-1,10)));
      log('retry',{attempt:failures,delayMs:wait,code:workerCode(error)});
      session?.close();onSession(null);
      await sleep(wait,signal);
    }finally{session?.close();onSession(null);}
  }
}
