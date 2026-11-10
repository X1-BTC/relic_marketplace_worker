import {HyperSyncLogSource} from '../indexer/hypersync-client.mjs';
import {sharedHyperSyncScheduler} from '../indexer/hypersync-scheduler.mjs';
import {decode,manifestDigest,validateManifest,readBatch,project} from '../indexer/marketplace.mjs';
import {businessRows} from '../indexer/business-tables.mjs';
import {marketplaceOnlyClient} from '../indexer/marketplace-only-client.mjs';
import {envioHyperRpcUrl,hybridReadClient,createRpcStatusFetch} from '../indexer/hybrid-rpc.mjs';

const DEPLOYMENT_BLOCK=99_777_330;
const RANGE_BLOCKS=10_000;
const COMPARE_BATCH=500;
const STREAM='relic-mainnet';
const MARKETPLACE='0xb87bbab928495bec8085eca758660790a0c67f9f';
const CONFIG_EVENTS=new Set(['PlatformFeeInfoUpdated','FlatPlatformFeeUpdated','PlatformFeeTypeUpdated','RoyaltyEngineUpdated']);
const LISTING_EVENTS=new Set(['NewListing','UpdatedListing','CancelledListing','NewSale']);
const OFFER_EVENTS=new Set(['NewOffer','CancelledOffer','AcceptedOffer']);
const SALE_EVENTS=new Set(['NewSale','AcceptedOffer']);
const CANCELLATION_EVENTS=new Set(['CancelledListing','CancelledOffer']);

function out(event,fields={}){process.stdout.write(`${JSON.stringify({time:new Date().toISOString(),event,...fields})}\n`);}
function requireEnv(name){const value=process.env[name];if(typeof value!=='string'||!value.trim())throw Error(`AUDIT_BINDING_MISSING:${name}`);return value;}
function parseConfig(){
  const manifest=validateManifest(JSON.parse(requireEnv('RM_MARKETPLACE_MANIFEST')));
  if(manifest.marketplace.toLowerCase()!==MARKETPLACE)throw Error('AUDIT_MANIFEST_MARKETPLACE_MISMATCH');
  const stream=process.env.RM_INDEXER_STREAM||STREAM;
  if(stream!==STREAM)throw Error('AUDIT_STREAM_MISMATCH');
  const mode=requireEnv('RM_DB_CONNECTION_MODE');
  if(!['direct','session'].includes(mode))throw Error('AUDIT_DB_MODE_INVALID');
  const host=requireEnv('RM_DB_HOST');
  if(mode==='session'&&!/^aws-[a-z0-9-]+\.pooler\.supabase\.com$/.test(host))throw Error('AUDIT_DB_HOST_INVALID');
  if(mode==='direct'&&host!=='db.colraxespxkbisfaazeo.supabase.co')throw Error('AUDIT_DB_HOST_INVALID');
  const user=requireEnv('RM_DB_USER');
  const suffix='.colraxespxkbisfaazeo';
  if(mode==='session'&&!user.endsWith(suffix))throw Error('AUDIT_DB_USER_INVALID');
  const ca=requireEnv('RM_DB_CA_CERT');
  if(!ca.includes('-----BEGIN CERTIFICATE-----')||!ca.includes('-----END CERTIFICATE-----')||ca.includes('PRIVATE KEY'))throw Error('AUDIT_DB_CA_INVALID');
  const port=Number(requireEnv('RM_DB_PORT'));
  if(port!==5432)throw Error('AUDIT_DB_PORT_INVALID');
  const database=requireEnv('RM_DB_NAME');
  if(!/^[a-zA-Z0-9_]+$/.test(database))throw Error('AUDIT_DB_NAME_INVALID');
  return {manifest,stream,token:requireEnv('ENVIO_API_TOKEN'),alchemyUrl:requireEnv('RM_MARKETPLACE_ALCHEMY_HTTP_URL'),connection:{
    host,port,database,user,password:requireEnv('RM_DB_PASSWORD'),
    ssl:{rejectUnauthorized:true,ca},
  }};
}

function classify(log,manifest){
  let eventName=null,args={};
  try{const result=decode(log,manifest);if(result){eventName=result.name;args=result.args;}}
  catch{return {eventName:`DECODE_ERROR:${log.topics[0]??'NO_TOPIC'}`,kind:'UNKNOWN',entityId:null};}
  if(!eventName)return {eventName:`UNKNOWN:${log.topics[0]??'NO_TOPIC'}`,kind:'UNKNOWN',entityId:null};
  if(CONFIG_EVENTS.has(eventName))return {eventName,kind:'CONFIG',entityId:null};
  if(LISTING_EVENTS.has(eventName))return {eventName,kind:'LISTING',entityId:String(args.listingId)};
  if(OFFER_EVENTS.has(eventName))return {eventName,kind:'OFFER',entityId:String(args.offerId)};
  return {eventName,kind:'OTHER',entityId:null};
}

const comparisonSql=`WITH source AS (
  SELECT * FROM unnest($1::text[],$2::integer[],$3::text[],$4::text[],$5::text[])
    AS s(transaction_hash,log_index,event_name,kind,entity_id)
)
SELECT s.transaction_hash,s.log_index,s.event_name,s.kind,s.entity_id,
  EXISTS(SELECT 1 FROM relic_market_indexer.events e WHERE e.stream=$6 AND e.transaction_hash=s.transaction_hash AND e.log_index=s.log_index) AS journal_present,
  EXISTS(SELECT 1 FROM public.relic_market_raw_events r WHERE r.chain_id=4663 AND r.transaction_hash=s.transaction_hash AND r.log_index=s.log_index) AS raw_present,
  EXISTS(SELECT 1 FROM public.relic_market_activity a WHERE a.chain_id=4663 AND a.marketplace_address=$7 AND a.transaction_hash=s.transaction_hash AND a.log_index=s.log_index AND a.ingest_source='LIVE') AS activity_present,
  CASE WHEN s.kind='LISTING' THEN EXISTS(SELECT 1 FROM public.relic_market_listings l WHERE l.chain_id=4663 AND l.marketplace_address=$7 AND l.listing_id=s.entity_id::numeric)
       WHEN s.kind='OFFER' THEN EXISTS(SELECT 1 FROM public.relic_market_offers o WHERE o.chain_id=4663 AND o.marketplace_address=$7 AND o.offer_id=s.entity_id::numeric)
       ELSE true END AS entity_present,
  CASE WHEN s.event_name IN ('NewSale','AcceptedOffer') THEN EXISTS(SELECT 1 FROM public.relic_market_settlements z WHERE z.chain_id=4663 AND z.marketplace_address=$7 AND z.transaction_hash=s.transaction_hash AND z.sale_log_index=s.log_index)
       WHEN s.kind='CONFIG' THEN EXISTS(SELECT 1 FROM public.relic_market_config_events c WHERE c.chain_id=4663 AND c.marketplace_address=$7 AND c.transaction_hash=s.transaction_hash AND c.log_index=s.log_index)
       ELSE true END AS specialized_row_present
FROM source s`;

async function readOnly(pool,work){
  const db=await pool.connect();let begun=false;
  try{
    await db.query('BEGIN READ ONLY');begun=true;
    const state=await db.query('SHOW transaction_read_only');
    if(state.rows[0]?.transaction_read_only!=='on')throw Error('AUDIT_READ_ONLY_GUARD_FAILED');
    await db.query('SET LOCAL ROLE relic_market_writer');
    const result=await work(db);
    await db.query('COMMIT');return result;
  }catch(error){if(begun)try{await db.query('ROLLBACK');}catch{}throw error;}
  finally{db.release();}
}

async function run(){
  const config=parseConfig();
  const pg=await import('pg');
  const pool=new pg.Pool({...config.connection,application_name:'relic-market-history-audit',max:1,connectionTimeoutMillis:15000,
    options:'-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=5000'});
  const metrics={requests:0,pages:0,retries:0,failures:0,verifiedBlocks:0,logs:0};
  const source=new HyperSyncLogSource({apiToken:config.token,scheduler:sharedHyperSyncScheduler,metrics:{
    request:()=>metrics.requests++,page:()=>metrics.pages++,retry:()=>metrics.retries++,failure:()=>metrics.failures++,
    verifiedRange:(blocks,logs)=>{metrics.verifiedBlocks+=blocks;metrics.logs+=logs;},
  }});
  const viem=await import('viem');
  const chain=viem.defineChain({id:4663,name:'Robinhood Mainnet',nativeCurrency:{name:'RHBTC',symbol:'RHBTC',decimals:18},rpcUrls:{default:{http:[config.alchemyUrl]}}});
  const envioFetch=createRpcStatusFetch();
  const envio=viem.createPublicClient({chain,transport:viem.http(envioHyperRpcUrl(config.token),{retryCount:0,timeout:15000,batch:false,fetchFn:envioFetch}),cacheTime:0});
  const alchemy=viem.createPublicClient({chain,transport:viem.http(config.alchemyUrl,{retryCount:0,timeout:15000,batch:false}),cacheTime:0});
  const client=marketplaceOnlyClient(hybridReadClient({envio,alchemy,rpcFacts:envioFetch.facts}),config.manifest);
  try{
    const checkpoint=await readOnly(pool,async db=>{
      const result=await db.query(`SELECT stream,chain_id,anchor_number,anchor_hash,block_number,block_hash,version,manifest_digest,manifest
        FROM relic_market_indexer.checkpoints WHERE stream=$1`,[config.stream]);
      if(result.rows.length!==1)throw Error('AUDIT_CHECKPOINT_NOT_UNIQUE');
      const row=result.rows[0];
      if(Number(row.chain_id)!==4663||manifestDigest(row.manifest)!==row.manifest_digest||row.manifest_digest!==manifestDigest(config.manifest))throw Error('AUDIT_CHECKPOINT_MANIFEST_MISMATCH');
      const boundary=await db.query('SELECT block_hash FROM relic_market_indexer.blocks WHERE stream=$1 AND block_number=$2',[config.stream,row.block_number]);
      return {...row,boundaryHash:boundary.rows[0]?.block_hash??null};
    });
    const endExclusive=Number(checkpoint.block_number)+1;
    if(!Number.isSafeInteger(endExclusive)||endExclusive<=DEPLOYMENT_BLOCK)throw Error('AUDIT_CHECKPOINT_RANGE_INVALID');
    if(!checkpoint.boundaryHash||checkpoint.boundaryHash.toLowerCase()!==checkpoint.block_hash.toLowerCase())throw Error('AUDIT_CHECKPOINT_BOUNDARY_MISMATCH');
    out('audit_started',{stream:config.stream,fromBlock:DEPLOYMENT_BLOCK,toBlockInclusive:Number(checkpoint.block_number),checkpointVersion:String(checkpoint.version),rangeBlocks:RANGE_BLOCKS,requestLimitPerMinute:10,readOnlyDatabase:true,receiptVerification:true});

    let cursor=DEPLOYMENT_BLOCK,lastHeader=null,rangeCount=0,pending=[];
    const countsByType={},missing=[],businessMissing={listings:0,offers:0,sales:0,cancellations:0,configuration:0,activity:0};
    let missingEventIdentities=0;
    const receiptEvents=[];
    const initialBoundary=await client.getBlock({blockNumber:BigInt(DEPLOYMENT_BLOCK-1)});
    if(!initialBoundary?.hash)throw Error('AUDIT_INITIAL_BOUNDARY_UNAVAILABLE');
    while(cursor<endExclusive){
      const rangeEnd=Math.min(cursor+RANGE_BLOCKS,endExclusive);
      const result=await source.getLogs({address:MARKETPLACE,fromBlock:cursor,toBlock:rangeEnd});
      if(!result.headers.length||result.headers[0].number!==cursor||result.headers.at(-1).number!==rangeEnd-1)throw Error('AUDIT_RANGE_HEADER_INCOMPLETE');
      if(lastHeader&&result.headers[0].parentHash!==lastHeader.hash)throw Error('AUDIT_CROSS_RANGE_REORG');
      lastHeader=result.headers.at(-1);
      for(const raw of result.logs){
        const item={...classify(raw,config.manifest),transactionHash:raw.transactionHash,logIndex:raw.logIndex,blockNumber:raw.blockNumber};
        countsByType[item.eventName]=(countsByType[item.eventName]??0)+1;
        // Flush in bounded SQL batches to keep Node heap use independent of total history.
        pending.push(item);
        if(pending.length>=COMPARE_BATCH){missingEventIdentities+=await compareBatch(pending,config,pool,missing,businessMissing);pending=[];}
      }
      const syntheticCheckpoint={...checkpoint,block_number:cursor-1,block_hash:cursor===DEPLOYMENT_BLOCK?initialBoundary.hash:result.headers[0].parentHash};
      const verified=await readBatch({client,manifest:config.manifest,checkpoint:syntheticCheckpoint,toBlock:rangeEnd-1,
        confirmations:12,head:BigInt(checkpoint.block_number)+12n,verifiedLogs:result,verifyContracts:false});
      receiptEvents.push(...verified.events);
      rangeCount++;
      if(rangeCount%10===0||rangeEnd===endExclusive)out('audit_progress',{rangeCount,throughBlock:rangeEnd-1,logs:metrics.logs,requests:metrics.requests,pages:metrics.pages,retries:metrics.retries});
      cursor=rangeEnd;
    }
    if(pending.length)missingEventIdentities+=await compareBatch(pending,config,pool,missing,businessMissing);
    if(lastHeader?.hash.toLowerCase()!==checkpoint.block_hash.toLowerCase())throw Error('AUDIT_CHECKPOINT_HASH_MISMATCH');
    const orderedReceipts=[...new Map(receiptEvents.map(e=>[`${e.transactionHash}:${e.logIndex}`,e])).values()].sort((a,b)=>a.blockNumber-b.blockNumber||a.transactionIndex-b.transactionIndex||a.logIndex-b.logIndex);
    const projected=project(orderedReceipts,config.manifest);
    const projectionRows=businessRows(orderedReceipts,projected,config.manifest,config.stream);
    const settlementStates={};
    for(const row of projectionRows.relic_market_settlements)settlementStates[row.reconcile_status]=(settlementStates[row.reconcile_status]??0)+1;
    out('audit_summary',{stream:config.stream,fromBlock:DEPLOYMENT_BLOCK,toBlockInclusive:Number(checkpoint.block_number),checkpointVersion:String(checkpoint.version),ranges:rangeCount,hyperSyncRequests:metrics.requests,hyperSyncPages:metrics.pages,hyperSyncRetries:metrics.retries,hyperSyncFailures:metrics.failures,blocksHeaderLinked:metrics.verifiedBlocks,logs:metrics.logs,countsByType,missingJournalOrRaw:missingEventIdentities,eventsWithAnyMissingProjection:missing.length,businessMissing,receiptVerifiedRelevantLogs:orderedReceipts.length,projectedSettlementStates:settlementStates,projectedPaymentTransfers:projectionRows.relic_market_payment_transfers.length,canonical:{fullParentLinkedHeaderSequence:true,sequenceAnchoredToStoredCheckpointHash:true,independentRpcComparison:false,receiptVerification:true},databaseWrites:0});
    for(const row of missing)out('audit_missing',row);
    out('audit_uncertainty',{receiptVerification:'Every MarketplaceV3 transaction receipt was checked for success, block/transaction identity, exact proxy log identity, and receipt-backed authorized NFT/PRINTER logs; existing projection and settlement logic was applied.',canonicalValidation:'HyperSync headers were parent-linked across pages/ranges; event blocks and each range endpoint were checked through the HyperRPC-preferred canonical read client; the final header matched the stored checkpoint hash. This is not an independent provider comparison unless Alchemy fallback was used.',replay:'Audit only; no rows or checkpoints were changed.'});
  }finally{await pool.end();}
}

async function compareBatch(items,config,pool,missing,businessMissing){
  const arrays=[items.map(x=>x.transactionHash),items.map(x=>x.logIndex),items.map(x=>x.eventName),items.map(x=>x.kind),items.map(x=>x.entityId??'')];
  const rows=await readOnly(pool,async db=>{
    const result=await db.query(comparisonSql,[...arrays,config.stream,MARKETPLACE]);return result.rows;
  });
  const byKey=new Map(rows.map(row=>[`${row.transaction_hash}:${row.log_index}`,row]));
  let missingIdentityCount=0;
  for(const item of items){
    const row=byKey.get(`${item.transactionHash}:${item.logIndex}`);
    if(!row)throw Error('AUDIT_COMPARE_RESULT_MISSING');
    const absent={};
    if(!row.journal_present)absent.journal=true;
    if(!row.raw_present)absent.raw=true;
    if(!row.journal_present||!row.raw_present)missingIdentityCount++;
    if(!row.activity_present){absent.activity=true;businessMissing.activity++;}
    if(item.kind==='LISTING'&&!row.entity_present){absent.listing=true;businessMissing.listings++;}
    if(item.kind==='OFFER'&&!row.entity_present){absent.offer=true;businessMissing.offers++;}
    if(SALE_EVENTS.has(item.eventName)&&!row.specialized_row_present){absent.settlement=true;businessMissing.sales++;}
    if(CANCELLATION_EVENTS.has(item.eventName)&&!row.activity_present){absent.cancellation=true;businessMissing.cancellations++;}
    if(item.kind==='CONFIG'&&!row.specialized_row_present){absent.configuration=true;businessMissing.configuration++;}
    if(Object.keys(absent).length)missing.push({eventName:item.eventName,transactionHash:item.transactionHash,logIndex:item.logIndex,blockNumber:item.blockNumber,missing:absent});
  }
  return missingIdentityCount;
}

run().catch(error=>{
  const missingBinding=typeof error?.message==='string'?error.message.match(/^AUDIT_BINDING_MISSING:([A-Z0-9_]+)$/):null;
  const code=missingBinding?'AUDIT_BINDING_MISSING':typeof error?.message==='string'&&/^AUDIT_[A-Z0-9_]+$/.test(error.message)?error.message:
    error?.code==='HYPERSYNC_HTTP_429'?'HYPERSYNC_HTTP_429':error?.code?.startsWith('HYPERSYNC_')?error.code:
    error?.code==='28P01'?'AUDIT_DATABASE_AUTH_FAILED':error?.code==='ETIMEDOUT'?'AUDIT_DATABASE_TIMEOUT':'AUDIT_FAILED';
  out('audit_failed',{code,binding:missingBinding?.[1],httpStatus:error?.httpStatus,fromBlock:error?.fromBlock,toBlock:error?.toBlock,nextBlock:error?.nextBlock});
  process.exitCode=1;
});
