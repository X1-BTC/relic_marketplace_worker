import {HyperSyncLogSource} from '../indexer/hypersync-client.mjs';
import {sharedHyperSyncScheduler} from '../indexer/hypersync-scheduler.mjs';
import {decode,manifestDigest,validateManifest,readBatch,project,normalizeLog,eventKey,json} from '../indexer/marketplace.mjs';
import {businessRows} from '../indexer/business-tables.mjs';
import {marketplaceOnlyClient} from '../indexer/marketplace-only-client.mjs';
import {envioHyperRpcUrl,hybridReadClient,createRpcStatusFetch} from '../indexer/hybrid-rpc.mjs';
import {pathToFileURL} from 'node:url';

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
export const TARGETED_TRANSACTIONS=Object.freeze([
  {block:100786855,hash:'0x60ecae79edc341c6e19d672ac819f8060c606f8707422aada60d0e7468a6b84f'},
  {block:100792665,hash:'0x1b2cab93fbfc64f44d5dd4d07cbbc80d213e8597a4ef7f44e5ee2cc9812c544a'},
  {block:100803626,hash:'0xab3008e3570893a8f2c956fa92385101292e4ecc6d274cbd8bab57e248d71fcc'},
  {block:100809473,hash:'0xd8da80961e134a5231cd91d54d4d759caa8056bb128cca791e32d8a6992c10'},
  {block:100813966,hash:'0xa37502403ec62ce5b7458e06b1e097d4f242dbd27b9aaa2a3c623437b58fc35f'},
  {block:100830176,hash:'0x68f725827ccdb782cd2a531eec1195da0f1ce61a912f50a9b2f18be2d3f59bef'},
  {block:101762900,hash:'0xba8739d4bcda7dda5da929190289ea37e438af71cb41989bf8ebdbb77b99bdf3'},
  {block:110532996,hash:'0x7905185ab75b3da485acdb22efaf88d5c724152e0a4010ef9e252db6b2a6607f'},
  {block:110541864,hash:'0xb3c8e654f87f33a1800080cf21435fc51cc7497ca775b6a0f26483d587dcd6c5'},
  {block:110560579,hash:'0x43d9b857246f88429a9e24d1e1d4b9dd620e0b69bd0c61255cfca57b5298699a'},
  {block:110693216,hash:'0xf9512ab91b8163fa11311b8ee0995b5fdfa393206282ab9fe80990539b832891'},
].map(x=>Object.freeze({...x,hash:x.hash.toLowerCase()})));

function out(event,fields={}){process.stdout.write(`${JSON.stringify({time:new Date().toISOString(),event,...fields})}\n`);}
function requireEnv(name){const value=process.env[name];if(typeof value!=='string'||!value.trim())throw Error(`AUDIT_BINDING_MISSING:${name}`);return value;}
function parseConfig({targeted=false}={}){
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
  return {manifest,stream,token:targeted?null:requireEnv('ENVIO_API_TOKEN'),alchemyUrl:requireEnv('RM_MARKETPLACE_ALCHEMY_HTTP_URL'),connection:{
    host,port,database,user,password:requireEnv('RM_DB_PASSWORD'),
    ssl:{rejectUnauthorized:true,ca},
  }};
}

const TARGET_RELEVANT_ADDRESSES=manifest=>new Set([manifest.marketplace,manifest.nft,manifest.printer].map(x=>x.toLowerCase()));
const asNumber=(v,label)=>{const n=Number(v);if(!Number.isSafeInteger(n)||n<0)throw Error(`TARGETED_INVALID_${label}`);return n;};

// Pure verification helpers are exported so the targeted path can be regression-tested
// without RPC or database credentials.
export function verifyTargetedReceipt({expected,receipt,canonicalBlock,head,manifest}){
  if(!receipt)throw Error('TARGETED_RECEIPT_MISSING');
  if(String(receipt.status).toLowerCase()!=='success'&&receipt.status!==1&&receipt.status!==1n)throw Error('TARGETED_RECEIPT_FAILED');
  if(receipt.transactionHash?.toLowerCase()!==expected.hash.toLowerCase())throw Error('TARGETED_RECEIPT_HASH_MISMATCH');
  const block=asNumber(receipt.blockNumber,'RECEIPT_BLOCK');
  if(block!==expected.block)throw Error('TARGETED_RECEIPT_BLOCK_MISMATCH');
  if(asNumber(canonicalBlock.number,'CANONICAL_BLOCK')!==expected.block)throw Error('TARGETED_CANONICAL_BLOCK_NUMBER_MISMATCH');
  if(!/^0x[0-9a-fA-F]{64}$/.test(canonicalBlock.hash)||!/^0x[0-9a-fA-F]{64}$/.test(canonicalBlock.parentHash??''))throw Error('TARGETED_CANONICAL_BLOCK_MALFORMED');
  if(!canonicalBlock?.hash||canonicalBlock.hash.toLowerCase()!==receipt.blockHash?.toLowerCase())throw Error('TARGETED_CANONICAL_HASH_MISMATCH');
  if(asNumber(head,'HEAD')-block<12)throw Error('TARGETED_CONFIRMATIONS_INSUFFICIENT');
  const txIndex=asNumber(receipt.transactionIndex,'TRANSACTION_INDEX');
  const seen=new Set(),normalized=[];let priorLogIndex=-1;
  for(const log of receipt.logs??[]){
    if(log.removed)throw Error('TARGETED_REMOVED_LOG');
    if(log.transactionHash?.toLowerCase()!==expected.hash.toLowerCase()||asNumber(log.blockNumber,'LOG_BLOCK')!==block||log.blockHash?.toLowerCase()!==canonicalBlock.hash.toLowerCase()||asNumber(log.transactionIndex,'LOG_TRANSACTION_INDEX')!==txIndex)throw Error('TARGETED_LOG_IDENTITY_MISMATCH');
    const li=asNumber(log.logIndex,'LOG_INDEX');if(seen.has(li))throw Error('TARGETED_DUPLICATE_LOG_INDEX');if(li<=priorLogIndex)throw Error('TARGETED_LOG_ORDER_INVALID');priorLogIndex=li;seen.add(li);
    const address=log.address?.toLowerCase();
    if(!TARGET_RELEVANT_ADDRESSES(manifest).has(address))continue;
    normalized.push(normalizeLog({...log,blockNumber:block,logIndex:li,transactionIndex:txIndex,blockTimestamp:new Date(Number(canonicalBlock.timestamp)*1000).toISOString()}));
  }
  const market=normalized.filter(x=>x.address===manifest.marketplace.toLowerCase());
  if(!market.length)throw Error('TARGETED_MARKETPLACE_LOG_MISSING');
  return normalized.sort((a,b)=>a.transactionIndex-b.transactionIndex||a.logIndex-b.logIndex);
}

export function targetedReplayBatches(verifiedReceipts,checkpoint,manifest){
  const byBlock=new Map();
  for(const item of verifiedReceipts){
    const number=asNumber(item.block.number,'REPLAY_BLOCK');
    const logs=item.logs??[];const prior=byBlock.get(number)??{block:{number,hash:item.block.hash.toLowerCase(),parentHash:item.block.parentHash.toLowerCase()},events:[]};
    if(prior.block.hash!==item.block.hash.toLowerCase()||prior.block.parentHash!==item.block.parentHash.toLowerCase())throw Error('TARGETED_BLOCK_CONFLICT');
    prior.events.push(...logs);byBlock.set(number,prior);
  }
  return [...byBlock.values()].sort((a,b)=>a.block.number-b.block.number).map(x=>{
    const seen=new Map();
    for(const e of x.events){const key=eventKey(e),old=seen.get(key);if(old&&json(old)!==json(e))throw Error('TARGETED_DUPLICATE_CONFLICT');seen.set(key,e);}
    const events=[...seen.values()].sort((a,b)=>a.blockNumber-b.blockNumber||a.transactionIndex-b.transactionIndex||a.logIndex-b.logIndex);
    return {expectedVersion:String(checkpoint.version),range:{fromBlock:x.block.number,toBlock:x.block.number},blocks:[x.block],events};
  });
}

export function verifyTargetedProjection(events,manifest){
  const ordered=[...events].sort((a,b)=>a.blockNumber-b.blockNumber||a.transactionIndex-b.transactionIndex||a.logIndex-b.logIndex);
  const state=project(ordered,manifest),rows=businessRows(ordered,state,manifest,STREAM);
  const ambiguous=rows.relic_market_settlements.filter(x=>x.reconcile_status!=='RECONCILED');
  return {state,rows,ambiguous};
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

async function runFull(){
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

async function runTargeted(){
  const config=parseConfig({targeted:true});
  const pg=await import('pg'),viem=await import('viem');
  const pool=new pg.Pool({...config.connection,application_name:'relic-market-targeted-receipt-audit',max:1,connectionTimeoutMillis:15000,
    options:'-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=5000'});
  const chain=viem.defineChain({id:4663,name:'Robinhood Mainnet',nativeCurrency:{name:'RHBTC',symbol:'RHBTC',decimals:18},rpcUrls:{default:{http:[config.alchemyUrl]}}});
  const alchemy=viem.createPublicClient({chain,transport:viem.http(config.alchemyUrl,{retryCount:0,timeout:20000,batch:false}),cacheTime:0});
  const client=marketplaceOnlyClient(alchemy,config.manifest);
  try{
    const [chainId,head,checkpoint,dbMeta]=await Promise.all([
      client.getChainId(),client.getBlockNumber(),
      readOnly(pool,async db=>{
        const q=await db.query(`SELECT stream,chain_id,anchor_number,anchor_hash,block_number,block_hash,version,manifest_digest,manifest FROM relic_market_indexer.checkpoints WHERE stream=$1`,[config.stream]);
        if(q.rows.length!==1)throw Error('AUDIT_CHECKPOINT_NOT_UNIQUE');
        const cp=q.rows[0];
        if(Number(cp.chain_id)!==4663||manifestDigest(cp.manifest)!==cp.manifest_digest||manifestDigest(config.manifest)!==cp.manifest_digest)throw Error('AUDIT_CHECKPOINT_MANIFEST_MISMATCH');
        if(Number(cp.block_number)<Math.max(...TARGETED_TRANSACTIONS.map(x=>x.block)))throw Error('AUDIT_CHECKPOINT_BEHIND_TARGETS');
        const boundary=await db.query('SELECT block_hash FROM relic_market_indexer.blocks WHERE stream=$1 AND block_number=$2',[config.stream,cp.block_number]);
        if(boundary.rows.length!==1||boundary.rows[0].block_hash.toLowerCase()!==cp.block_hash.toLowerCase())throw Error('AUDIT_CHECKPOINT_BOUNDARY_MISMATCH');
        return cp;
      }),
      readOnly(pool,async db=>{
        const rels=['relic_market_indexer.checkpoints','relic_market_indexer.blocks','relic_market_indexer.events','relic_market_indexer.entity_undo','public.relic_market_contracts','public.relic_market_raw_events','public.relic_market_listings','public.relic_market_offers','public.relic_market_activity','public.relic_market_settlements','public.relic_market_payment_transfers','public.relic_market_config_events'];
        const r=[];for(const relation of rels){const x=await db.query('SELECT to_regclass($1) IS NOT NULL AS present',[relation]);r.push({relation,present:x.rows[0]?.present===true});}
        if(r.some(x=>!x.present))throw Error('AUDIT_REQUIRED_RELATION_MISSING');
        const registry=await db.query('SELECT role,contract_address,start_block,enabled FROM public.relic_market_contracts WHERE chain_id=4663 AND contract_address=ANY($1::text[])',[[config.manifest.marketplace,config.manifest.nft,config.manifest.printer].map(x=>x.toLowerCase())]);
        const expected={MARKETPLACE_V3:config.manifest.marketplace.toLowerCase(),RELIC_NFT:config.manifest.nft.toLowerCase(),PRINTER:config.manifest.printer.toLowerCase()};
        for(const [role,address] of Object.entries(expected)){const x=registry.rows.find(y=>y.role===role&&y.contract_address===address&&y.enabled===true);if(!x||BigInt(x.start_block)>BigInt(TARGETED_TRANSACTIONS[0].block))throw Error('AUDIT_REGISTRY_MISMATCH');}
        const cfg=await db.query('SELECT count(*)::text AS count FROM public.relic_market_config_events WHERE chain_id=4663 AND marketplace_address=$1',[config.manifest.marketplace.toLowerCase()]);
        return {registry:registry.rows.map(x=>({role:x.role,contract_address:x.contract_address,start_block:String(x.start_block),enabled:x.enabled})),existingConfigEventRows:cfg.rows[0]?.count??'0'};
      }),
    ]);
    if(chainId!==4663)throw Error('AUDIT_CHAIN_ID_MISMATCH');
    const verified=[];
    for(const expected of TARGETED_TRANSACTIONS){
      // Exactly one receipt lookup per supplied hash; no eth_getLogs requests.
      const receipt=await client.getTransactionReceipt({hash:expected.hash});
      const canonicalBlock=await client.getBlock({blockNumber:BigInt(expected.block)});
      const logs=verifyTargetedReceipt({expected,receipt,canonicalBlock,head,manifest:config.manifest});
      verified.push({expected,receipt,block:canonicalBlock,logs});
    }
    const allLogs=verified.flatMap(x=>x.logs).sort((a,b)=>a.blockNumber-b.blockNumber||a.transactionIndex-b.transactionIndex||a.logIndex-b.logIndex);
    const unique=new Map();for(const log of allLogs){const key=eventKey(log),old=unique.get(key);if(old&&JSON.stringify(old)!==JSON.stringify(log))throw Error('TARGETED_DUPLICATE_CONFLICT');unique.set(key,log);}
    const events=[...unique.values()].sort((a,b)=>a.blockNumber-b.blockNumber||a.transactionIndex-b.transactionIndex||a.logIndex-b.logIndex);
    const classifications=events.map(log=>({log,...classify(log,config.manifest)}));
    const typeCounts={};for(const x of classifications)typeCounts[x.eventName]=(typeCounts[x.eventName]??0)+1;
    const projection=verifyTargetedProjection(events,config.manifest);
    const replayBatches=targetedReplayBatches(verified.map(x=>({block:x.block,logs:x.logs})),checkpoint,config.manifest);
    const source=classifications.map(x=>({transactionHash:x.log.transactionHash,logIndex:x.log.logIndex,blockNumber:x.log.blockNumber,eventName:x.eventName,kind:x.kind,entityId:x.entityId??'',address:x.log.address}));
    const dbCompare=await readOnly(pool,async db=>{
      const arrays=[source.map(x=>x.transactionHash),source.map(x=>x.logIndex),source.map(x=>x.blockNumber),source.map(x=>x.eventName),source.map(x=>x.kind),source.map(x=>x.entityId),source.map(x=>x.address)];
      const q=await db.query(`WITH s AS (SELECT * FROM unnest($1::text[],$2::integer[],$3::bigint[],$4::text[],$5::text[],$6::text[],$7::text[]) AS x(transaction_hash,log_index,block_number,event_name,kind,entity_id,address))
        SELECT s.*,
          EXISTS(SELECT 1 FROM relic_market_indexer.events e WHERE e.stream=$8 AND e.transaction_hash=s.transaction_hash AND e.log_index=s.log_index) journal_present,
          EXISTS(SELECT 1 FROM public.relic_market_raw_events r WHERE r.chain_id=4663 AND r.transaction_hash=s.transaction_hash AND r.log_index=s.log_index AND lower(r.contract_address)=s.address) raw_present,
          CASE WHEN s.address=$9 OR s.address=$10 THEN EXISTS(SELECT 1 FROM public.relic_market_activity a WHERE a.chain_id=4663 AND a.marketplace_address=$9 AND a.transaction_hash=s.transaction_hash AND a.log_index=s.log_index AND a.ingest_source='LIVE') ELSE true END activity_present,
          CASE WHEN s.kind='LISTING' THEN EXISTS(SELECT 1 FROM public.relic_market_listings l WHERE l.chain_id=4663 AND l.marketplace_address=$9 AND l.listing_id=s.entity_id::numeric AND (l.last_block_number>s.block_number OR (l.last_block_number=s.block_number AND l.last_log_index>=s.log_index)))
               WHEN s.kind='OFFER' THEN EXISTS(SELECT 1 FROM public.relic_market_offers o WHERE o.chain_id=4663 AND o.marketplace_address=$9 AND o.offer_id=s.entity_id::numeric AND (o.last_block_number>s.block_number OR (o.last_block_number=s.block_number AND o.last_log_index>=s.log_index))) ELSE true END entity_present,
          CASE WHEN s.event_name IN ('NewSale','AcceptedOffer') THEN EXISTS(SELECT 1 FROM public.relic_market_settlements z WHERE z.chain_id=4663 AND z.marketplace_address=$9 AND z.transaction_hash=s.transaction_hash AND z.sale_log_index=s.log_index)
               WHEN s.kind='CONFIG' THEN EXISTS(SELECT 1 FROM public.relic_market_config_events c WHERE c.chain_id=4663 AND c.marketplace_address=$9 AND c.transaction_hash=s.transaction_hash AND c.log_index=s.log_index) ELSE true END specialized_present,
          CASE WHEN s.address=$10 THEN EXISTS(SELECT 1 FROM public.relic_market_payment_transfers p WHERE p.chain_id=4663 AND p.transaction_hash=s.transaction_hash AND p.log_index=s.log_index) ELSE true END payment_present
        FROM s ORDER BY s.block_number,s.transaction_hash,s.log_index`,[...arrays,config.stream,config.manifest.marketplace.toLowerCase(),config.manifest.printer.toLowerCase()]);
      return q.rows;
    });
    const missing=dbCompare.filter(x=>!x.journal_present||!x.raw_present||!x.activity_present||!x.entity_present||!x.specialized_present||!x.payment_present).map(x=>({blockNumber:String(x.block_number),transactionHash:x.transaction_hash,logIndex:x.log_index,eventName:x.event_name,kind:x.kind,missing:{...(!x.journal_present&&{journal:true}),...(!x.raw_present&&{raw:true}),...(!x.activity_present&&{activity:true}),...(!x.entity_present&&{entity:true}),...(!x.specialized_present&&{specialized:true}),...(!x.payment_present&&{paymentTransfer:true})}}));
    const types={};for(const x of classifications){const d=decode(x.log,config.manifest);types[d?.name??x.eventName]=(types[d?.name??x.eventName]??0)+1;}
    const receiptSummary=verified.map(x=>({block:x.expected.block,transactionHash:x.expected.hash,status:'success',blockHash:x.block.hash,transactionIndex:Number(x.receipt.transactionIndex),marketplaceLogs:x.logs.filter(l=>l.address===config.manifest.marketplace.toLowerCase()).length,relevantLogs:x.logs.length}));
    const replayReady=projection.ambiguous.length===0&&missing.length>0&&missing.every(x=>x.missing.journal||x.missing.raw);
    out('targeted_audit_summary',{mode:'targeted_receipts_only',chainId,head:String(head),confirmationsRequired:12,transactions:receiptSummary,eventCountsByDecodedType:types,receiptLogCount:events.length,duplicateIdentitiesRemoved:allLogs.length-events.length,missingDatabaseIdentities:missing,ambiguousSettlements:projection.ambiguous.map(x=>({transactionHash:x.transaction_hash,logIndex:x.sale_log_index,status:x.reconcile_status})),existingDatabase:dbMeta,checkpoint:{blockNumber:String(checkpoint.block_number),version:String(checkpoint.version)},configurationSource:'RM_MARKETPLACE_MANIFEST validated and digest matched against checkpoint; contract registry independently verified',rpc:{receiptLookups:11,canonicalBlockReads:11,ethGetLogs:0},replayReady,replayNotReadyReason:replayReady?null:projection.ambiguous.length?'AMBIGUOUS_SETTLEMENT':'NO_MISSING_EVENT_IDENTITY_OR_PROJECTION_ONLY_MISSING',replayBatches,fullHistoryAdministrativeCompleteness:'NOT_ESTABLISHED_BY_TARGETED_RECEIPTS',databaseWrites:0});
  }finally{await pool.end();}
}

const mode=process.argv.includes('--full')?'full':'targeted';
const run=mode==='full'?runFull:runTargeted;
if(process.argv.includes('--help')){out('usage',{targeted:'node scripts/audit-marketplace-history.mjs --targeted',full:'node scripts/audit-marketplace-history.mjs --full (not for targeted recovery)'});}
else if(import.meta.url===pathToFileURL(process.argv[1]??'').href)run().catch(error=>{
  const missingBinding=typeof error?.message==='string'?error.message.match(/^AUDIT_BINDING_MISSING:([A-Z0-9_]+)$/):null;
  const code=missingBinding?'AUDIT_BINDING_MISSING':typeof error?.message==='string'&&/^AUDIT_[A-Z0-9_]+$/.test(error.message)?error.message:
    error?.code==='HYPERSYNC_HTTP_429'?'HYPERSYNC_HTTP_429':error?.code?.startsWith('HYPERSYNC_')?error.code:
    error?.code==='28P01'?'AUDIT_DATABASE_AUTH_FAILED':error?.code==='ETIMEDOUT'?'AUDIT_DATABASE_TIMEOUT':'AUDIT_FAILED';
  out('audit_failed',{code,binding:missingBinding?.[1],httpStatus:error?.httpStatus,fromBlock:error?.fromBlock,toBlock:error?.toBlock,nextBlock:error?.nextBlock});
  process.exitCode=1;
});
