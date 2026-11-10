import {pathToFileURL} from 'node:url';
import {readWorkerConfig} from '../indexer/worker-config.mjs';
import {createRpcStatusFetch} from '../indexer/hybrid-rpc.mjs';
import {verifyChain,manifestDigest,json} from '../indexer/marketplace.mjs';
import {acquireWorkerSession} from '../indexer/worker-session.mjs';
import {targetManifest,priorManifest,checkpointSnapshot,applyManifestMigration,assertMaintenanceClear,STREAM} from './manifest-migration-core.mjs';

function emit(event,fields={}){process.stdout.write(`${JSON.stringify({time:new Date().toISOString(),event,...fields})}\n`);}
function must(ok,code){if(!ok)throw Error(code);}
async function readOnly(pool,work){
  const db=await pool.connect();let open=false;
  try{await db.query('BEGIN READ ONLY');open=true;await db.query('SET LOCAL ROLE relic_market_writer');const result=await work(db);await db.query('COMMIT');open=false;return result;}
  catch(e){if(open)try{await db.query('ROLLBACK');}catch{}throw e;}finally{db.release();}
}
async function main(){
  const apply=process.argv.includes('--apply');
  const rollback=process.argv.includes('--rollback');
  if(apply){must(process.env[rollback?'RM_ALLOW_PRODUCTION_MANIFEST_ROLLBACK':'RM_ALLOW_PRODUCTION_MANIFEST_MIGRATION']==='true','PRODUCTION_MANIFEST_OPERATION_EXPLICIT_ENV_APPROVAL_REQUIRED');must(process.argv.includes('--maintenance-confirmation=STOPPED_AND_READY'),'MAINTENANCE_CONFIRMATION_REQUIRED');}
  const config=readWorkerConfig(process.env);must(config.enabled,'WORKER_DISABLED');
  const pg=await import('pg'),viem=await import('viem');
  const raw=new pg.Pool({...config.connection,application_name:'relic-market-manifest-migration',max:1,connectionTimeoutMillis:15000,
    options:'-c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000'});
  raw.on('error',()=>{});
  const leasedPool={async connect(){const db=await raw.connect();try{await db.query('SET ROLE relic_market_writer');await db.query('SET search_path=pg_catalog');return db;}catch{db.release(true);throw Error('RECOVERY_DATABASE_SESSION_UNAVAILABLE');}},end:()=>raw.end()};
  const chain=viem.defineChain({id:4663,name:'Robinhood Mainnet',nativeCurrency:{name:'RHBTC',symbol:'RHBTC',decimals:18},rpcUrls:{default:{http:[config.httpRpcUrl]}}});
  const rpc=viem.createPublicClient({chain,transport:viem.http(config.httpRpcUrl,{retryCount:0,timeout:15000,batch:false,fetchFn:createRpcStatusFetch()}),cacheTime:0});
  let session=null;
  try{
    const before=await readOnly(raw,async db=>{
      const cols=await db.query(`SELECT column_name FROM information_schema.columns WHERE table_schema='relic_market_indexer' AND table_name='checkpoints'`);
      const available=new Set(cols.rows.map(x=>x.column_name));
      must(['stream','chain_id','anchor_number','anchor_hash','block_number','block_hash','version','manifest_digest','manifest'].every(x=>available.has(x)),'CHECKPOINT_SCHEMA_UNSUPPORTED');
      const q=await db.query(`SELECT stream,chain_id,anchor_number,anchor_hash,block_number,block_hash,version,manifest_digest,manifest
        FROM relic_market_indexer.checkpoints WHERE stream=$1`,[STREAM]);
      must(q.rows.length===1,'CHECKPOINT_NOT_UNIQUE');
      const cp=checkpointSnapshot(q.rows[0]);
      const lock=await db.query(`SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted
        AND classid=4663::oid AND objid=((hashtext('relic_market_indexer:marketplace-v3')::bigint & 4294967295)::oid) AND objsubid=2) AS held`);
      const writers=await db.query(`SELECT count(*)::text AS count FROM pg_stat_activity WHERE datname=current_database()
        AND application_name='relic-marketplace-worker' AND pid<>pg_backend_pid()`);
      return {cp,lockHeld:lock.rows[0]?.held===true,workerSessions:writers.rows[0]?.count??'0'};
    });
    must(before.cp.manifest_digest===manifestDigest(before.cp.manifest),'CHECKPOINT_MANIFEST_DIGEST_INVALID');
    const plan=rollback?priorManifest(before.cp.manifest):targetManifest(before.cp.manifest);
    must(config.manifest.chainId===4663&&config.manifest.marketplace.toLowerCase()===plan.target.marketplace.toLowerCase(),'RENDER_MANIFEST_CONTRACT_MISMATCH');
    const envDigest=manifestDigest(config.manifest);
    must(envDigest===plan.currentDigest||envDigest===plan.targetDigest,'RENDER_MANIFEST_UNRELATED_TO_CHECKPOINT');
    const renderAligned=envDigest===plan.targetDigest;
    await verifyChain(rpc,plan.current,Number(before.cp.block_number));
    const canonicalCheckpoint=await rpc.getBlock({blockNumber:BigInt(before.cp.block_number)});
    must(canonicalCheckpoint.hash.toLowerCase()===before.cp.block_hash,'CHECKPOINT_BLOCK_NOT_CANONICAL');
    const result={stream:STREAM,chainId:4663,direction:rollback?'rollback':'forward',mode:apply?'apply':'dry-run',before:{blockNumber:before.cp.block_number,blockHash:before.cp.block_hash,version:before.cp.version,manifestDigest:plan.currentDigest,feeRecipients:plan.current.feeRecipients},after:{blockNumber:before.cp.block_number,blockHash:before.cp.block_hash,version:before.cp.version,manifestDigest:plan.targetDigest,feeRecipients:plan.target.feeRecipients},beforeManifest:plan.current,targetManifest:plan.target,renderManifestAligned:renderAligned,contractHashesVerified:true,advisoryLockHeldBeforeMigration:before.lockHeld,otherNamedWorkerSessions:before.workerSessions,checkpointMutation:'manifest and manifest_digest only'};
    if(!apply){emit('manifest_migration_preflight',{...result,writePerformed:false,canApply:!before.lockHeld&&before.workerSessions==='0'&&renderAligned});return;}
    must(renderAligned,'RENDER_MANIFEST_MUST_BE_UPDATED_BEFORE_MIGRATION');
    assertMaintenanceClear({lockHeld:before.lockHeld,workerSessions:before.workerSessions});
    session=await acquireWorkerSession(leasedPool,{login:config.login,database:config.connection.database,signal:new AbortController().signal});
    must(session,'WORKER_ADVISORY_LOCK_BUSY');
    const client=await session.pool.connect();
    try{
      const migrated=await applyManifestMigration(client,{expected:before.cp,target:plan.target,digest:plan.targetDigest,direction:rollback?'rollback':'forward'});
      emit('manifest_migration_complete',{...result,changed:migrated.changed,writePerformed:migrated.changed,after:{blockNumber:migrated.checkpoint.block_number,blockHash:migrated.checkpoint.block_hash,version:migrated.checkpoint.version,manifestDigest:migrated.checkpoint.manifest_digest,feeRecipients:migrated.checkpoint.manifest.feeRecipients}});
    }finally{client.release();}
  }finally{session?.close();await raw.end();}
}

if(import.meta.url===pathToFileURL(process.argv[1]??'').href)main().catch(error=>{emit('manifest_migration_failed',{code:/^[A-Z0-9_]+$/.test(error.message)?error.message:'MANIFEST_MIGRATION_FAILED'});process.exitCode=1;});
