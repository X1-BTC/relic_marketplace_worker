import assert from 'node:assert/strict';
import {Interface,keccak256} from 'ethers';
import {PRODUCTION_CONFIG} from '../shared/production-config.mjs';
import {marketEvents,readBatch,manifestDigest,decode,project} from '../indexer/marketplace.mjs';
import {businessRows,rawRow} from '../indexer/business-tables.mjs';
import {PostgresMarketplaceWriter} from '../indexer/postgres-writer.mjs';
import {HyperSyncLogSource} from '../indexer/hypersync-client.mjs';
import {HyperSyncRequestScheduler} from '../indexer/hypersync-scheduler.mjs';
import {envioHyperRpcUrl,hybridReadClient,createRpcStatusFetch,RpcFallbackScheduler} from '../indexer/hybrid-rpc.mjs';
import {indexOnce} from '../indexer/run-once.mjs';
import {checkpointBehindConfirmedHead} from '../indexer/worker-loop.mjs';

const H=n=>`0x${BigInt(n).toString(16).padStart(64,'0')}`;
const manifest={chainId:4663,nft:PRODUCTION_CONFIG.nftAddress,printer:PRODUCTION_CONFIG.currencyAddress,
  marketplace:'0xb87bbab928495bec8085eca758660790a0c67f9f',printerDecimals:18,
  codeHashes:{nft:keccak256('0x01'),printer:keccak256('0x01'),marketplace:keccak256('0x01')},feeRecipients:[]};
const base=41_798_500;
const checkpoint={chain_id:4663,manifest,manifest_digest:manifestDigest(manifest),anchor_number:base,
  anchor_hash:H(base),block_number:base,block_hash:H(base),version:'7'};
const block=(n,hash=H(n))=>({number:BigInt(n),hash,parentHash:H(n-1),timestamp:BigInt(1_700_000_000+n)});
function testScheduler(limit=10){let time=0;return {scheduler:new HyperSyncRequestScheduler({limit,windowMs:60000,now:()=>time,sleep:async ms=>{time+=ms;},random:()=>0}),now:()=>time};}
assert.equal(checkpointBehindConfirmedHead(120,108,12),false,'caught-up checkpoint is not rescanned');
assert.equal(checkpointBehindConfirmedHead(121,108,12),true,'only a confirmed gap starts range processing');

// Incident fixtures: these exact on-chain topic0 values and indexed identities
// were read from the first two supplied proxy receipts. The tuple payload is encoded as a
// deterministic supported listing fixture because the receipt data itself is
// not part of the incident evidence available to this local test.
{
  const known=[
    {block:110_532_996,tx:'0x7905185ab75b3da485acdb22efaf88d5c724152e0a4010ef9e252db6b2a6607f',id:'1'},
    {block:110_541_864,tx:'0xb3c8e654f87f33a1800080cf21435fc51cc7497ca775b6a0f26483d587dcd6c5',id:'2'},
  ];
  const creator='0x591f30b91573a072be38c0c6e42a04a48eab817b';
  for(const item of known){
    const fragment=marketEvents.getEvent('NewListing');
    assert.equal(fragment.topicHash,'0xef309e3999c4dd6a4c1e4af6221896b7e5ccf9e7fc4fe5b218b883ce9190d7ad');
    const tuple={listingId:item.id,tokenId:item.id,quantity:'1',pricePerToken:'1000000000000000000',
      startTimestamp:'1700000000',endTimestamp:((1n<<128n)-1n).toString(),listingCreator:creator,
      assetContract:manifest.nft,currency:manifest.printer,tokenType:'0',status:'1',reserved:false};
    const encoded=marketEvents.encodeEventLog(fragment,[creator,item.id,manifest.nft,tuple]);
    const blockHash=H(item.block), parentHash=H(item.block-1);
    const log={address:manifest.marketplace,blockNumber:item.block,blockHash,transactionHash:item.tx,
      transactionIndex:0,logIndex:0,topics:encoded.topics,data:encoded.data,removed:false,
      blockTimestamp:'2026-11-10T00:00:00.000Z'};
    assert.equal(decode(log,manifest)?.name,'NewListing','observed incident topic is a listing event, not a configuration event');
    const receiptLog={...log,blockNumber:BigInt(item.block),blockTimestamp:null};
    const readClient={
      async getBlock({blockNumber}){const n=Number(blockNumber);return {number:BigInt(n),hash:H(n),parentHash:H(n-1),timestamp:BigInt(1_700_000_000)};},
      async getTransactionReceipt({hash}){return {status:'success',transactionHash:hash,blockNumber:BigInt(item.block),blockHash,transactionIndex:0,logs:[receiptLog]};},
    };
    const verified=await readBatch({client:readClient,manifest,
      checkpoint:{...checkpoint,block_number:item.block-1,block_hash:parentHash},toBlock:item.block,
      head:BigInt(item.block+12),verifyContracts:false,
      logSource:{async getLogs(){return {logs:[log],headers:[{number:item.block,hash:blockHash,parentHash}],rollbackGuards:[]};}}});
    assert.equal(verified.events.length,1,'known transaction survives bounded HyperSync and receipt verification');
    assert.equal(verified.events[0].transactionHash,item.tx);
    assert.equal(verified.events[0].blockNumber,item.block);
    const projected=project([log],manifest);
    const rows=businessRows([log],projected,manifest,'relic-mainnet');
    const raw=rawRow(log,manifest);
    assert.equal(raw.event_name,'NewListing');
    assert.equal(rows.relic_market_listings.length,1);
    assert.equal(rows.relic_market_activity.length,1);
    assert.equal(rows.relic_market_config_events.length,0);
    assert.equal(rows.relic_market_listings[0].listing_id,item.id);
    assert.equal(blockHash,H(item.block));
    assert.equal(parentHash,H(item.block-1));
  }
}

// Projection regression coverage for listing/offer transitions and receipt-backed
// NFT/PRINTER settlement reconciliation.
{
  const seller='0x0000000000000000000000000000000000000011',buyer='0x0000000000000000000000000000000000000022';
  const mk=(name,args,index,tx,blockNo)=>{const encoded=marketEvents.encodeEventLog(marketEvents.getEvent(name),args);return {address:manifest.marketplace,blockNumber:blockNo,blockHash:H(blockNo),transactionHash:tx,transactionIndex:0,logIndex:index,topics:encoded.topics,data:encoded.data,removed:false,blockTimestamp:'2026-11-10T00:00:00.000Z'};};
  const other=(contract,iface,args,index,tx,blockNo)=>{const encoded=iface.encodeEventLog(iface.getEvent('Transfer'),args);return {address:contract,blockNumber:blockNo,blockHash:H(blockNo),transactionHash:tx,transactionIndex:0,logIndex:index,topics:encoded.topics,data:encoded.data,removed:false,blockTimestamp:'2026-11-10T00:00:00.000Z'};};
  const listing=(id,token,price)=>({listingId:String(id),tokenId:String(token),quantity:'1',pricePerToken:String(price),startTimestamp:'1700000000',endTimestamp:((1n<<128n)-1n).toString(),listingCreator:seller,assetContract:manifest.nft,currency:manifest.printer,tokenType:'0',status:'1',reserved:false});
  const offer=(id,token,price)=>({offerId:String(id),tokenId:String(token),quantity:'1',totalPrice:String(price),expirationTimestamp:((1n<<256n)-1n).toString(),offeror:buyer,assetContract:manifest.nft,currency:manifest.printer,tokenType:'0',status:'1'});
  const printer=new Interface(['event Transfer(address indexed from,address indexed to,uint256 value)']);
  const nft=new Interface(['event Transfer(address indexed from,address indexed to,uint256 indexed tokenId)']);
  const txSale=H(9901),txOffer=H(9902);
  const logs=[
    mk('NewListing',[seller,'3',manifest.nft,listing(3,42,100)],0,H(9800),base+1),
    mk('UpdatedListing',[seller,'3',manifest.nft,listing(3,42,120)],0,H(9801),base+2),
    mk('NewSale',[seller,'3',manifest.nft,'42',buyer,'1','120'],0,txSale,base+3),
    other(manifest.nft,nft,[seller,buyer,'42'],1,txSale,base+3),other(manifest.printer,printer,[buyer,seller,'120'],2,txSale,base+3),
    mk('NewListing',[seller,'4',manifest.nft,listing(4,43,110)],0,H(9802),base+4),mk('CancelledListing',[seller,'4'],0,H(9803),base+5),
    mk('NewOffer',[buyer,'9',manifest.nft,offer(9,99,200)],0,H(9804),base+6),
    mk('AcceptedOffer',[buyer,'9',manifest.nft,'99',seller,'1','200'],0,txOffer,base+7),
    other(manifest.nft,nft,[seller,buyer,'99'],1,txOffer,base+7),other(manifest.printer,printer,[buyer,seller,'200'],2,txOffer,base+7),
    mk('NewOffer',[buyer,'10',manifest.nft,offer(10,100,300)],0,H(9805),base+8),mk('CancelledOffer',[buyer,'10'],0,H(9806),base+9),
  ];
  const rows=businessRows(logs,project(logs,manifest),manifest,'relic-mainnet');
  assert.equal(rows.relic_market_listings.find(row=>row.listing_id==='3').status,'SOLD');
  assert.equal(rows.relic_market_listings.find(row=>row.listing_id==='4').status,'CANCELLED');
  assert.equal(rows.relic_market_offers.find(row=>row.offer_id==='9').status,'ACCEPTED');
  assert.equal(rows.relic_market_offers.find(row=>row.offer_id==='10').status,'CANCELLED');
  assert.deepEqual(rows.relic_market_settlements.map(row=>row.reconcile_status),['RECONCILED','RECONCILED']);
  assert.equal(rows.relic_market_payment_transfers.length,2);
}

// HyperRPC handles supported block and receipt methods; unsupported or failed
// reads fall back through a bounded Alchemy path and retain safe status data.
{
  const calls={envio:[],alchemy:[]};
  const make=(side,failures={})=>Object.fromEntries(['getBlockNumber','getBlock','getTransactionReceipt','getChainId','getBytecode','readContract'].map(name=>[name,async args=>{
    calls[side].push(name);
    if(failures[name])throw failures[name];
    return `${side}:${name}`;
  }]));
  const rpc=hybridReadClient({envio:make('envio'),alchemy:make('alchemy')});
  assert.equal(await rpc.getBlockNumber(),'envio:getBlockNumber');
  assert.equal(await rpc.getBlock({blockNumber:1n}),'envio:getBlock');
  assert.equal(await rpc.getTransactionReceipt({hash:'0x01'}),'envio:getTransactionReceipt');
  assert.equal(calls.alchemy.length,0,'supported HyperRPC reads do not use Alchemy');
  assert.equal(await rpc.getChainId(),'alchemy:getChainId');
  assert.equal(await rpc.getBytecode({address:'0x01'}),'alchemy:getBytecode');
  assert.equal(await rpc.readContract({address:'0x01'}),'alchemy:readContract');

  const unsupported=Object.assign(new Error('method not found'),{code:-32601});
  const directFallback={async request(task){return task();}};
  const fallback=hybridReadClient({envio:make('envio',{getBlockNumber:unsupported}),alchemy:make('alchemy'),fallbackScheduler:directFallback});
  assert.equal(await fallback.getBlockNumber(),'alchemy:getBlockNumber');
  assert.equal(await fallback.getBlockNumber(),'alchemy:getBlockNumber');
  assert.equal(calls.envio.filter(name=>name==='getBlockNumber').length,2,'unsupported capability is cached after first failure');

  const transient=hybridReadClient({envio:make('envio',{getBlockNumber:new Error('temporary failure')}),alchemy:make('alchemy'),fallbackScheduler:directFallback});
  assert.equal(await transient.getBlockNumber(),'alchemy:getBlockNumber','transient HyperRPC failures fail over to Alchemy');
  assert.equal(await transient.getBlockNumber(),'alchemy:getBlockNumber','the cooldown avoids a rapid retry loop against HyperRPC');
  assert.equal(calls.envio.filter(name=>name==='getBlockNumber').length,3,'unsupported is cached while a transient outage is retried only after its cooldown');
  assert.equal(calls.alchemy.filter(name=>name==='getBlockNumber').length,4,'each failed or cooling-down read uses Alchemy');
  let reported=null;
  const authFailure=hybridReadClient({envio:make('envio',{getBlockNumber:Object.assign(new Error('unauthorized'),{status:401})}),
    alchemy:make('alchemy'),fallbackScheduler:directFallback,rpcFacts:()=>({httpStatus:401,jsonRpcCode:-32001}),
    onCapability:(method,state,facts)=>{if(state==='unavailable')reported={method,...facts};}});
  assert.equal(await authFailure.getBlockNumber(),'alchemy:getBlockNumber','HyperRPC authorization failure does not stop indexing');
  assert.deepEqual(reported,{method:'eth_blockNumber',httpStatus:401,jsonRpcCode:-32001});
  assert.equal(envioHyperRpcUrl('token/a'),'https://4663.rpc.hypersync.xyz/token%2Fa');

  const clock={time:0};let fallbackCalls=0;
  const limiter=new RpcFallbackScheduler({limit:10,windowMs:60000,now:()=>clock.time,sleep:async ms=>{clock.time+=ms;}});
  await Promise.all(Array.from({length:11},()=>limiter.request(async()=>{fallbackCalls++;})));
  assert.equal(fallbackCalls,11);
  assert.ok(clock.time>=60000,'Alchemy fallback is globally bounded to ten reads per rolling minute');

  let observed=null;
  const statusFetch=createRpcStatusFetch(facts=>{observed=facts;},async()=>({status:403,clone:()=>({json:async()=>({error:{code:-32001,message:'private provider details'}})})}));
  await statusFetch('https://credential-bearing.invalid/token',{method:'POST'});
  assert.deepEqual(observed,{httpStatus:403,jsonRpcCode:-32001},'diagnostics expose only HTTP status and numeric JSON-RPC code');
}
{
  const {client,calls}=rpc();
  const writer={manifest,async checkpoint(){return checkpoint;},async commit(){throw Error('caught-up state must not commit');}};
  const result=await indexOnce({client,writer,confirmations:12,batchSize:10000,checkpoint,head:BigInt(base+12),verifyIdle:false,
    logSource:{async getLogs(){throw Error('caught-up state must not query HyperSync');}}});
  assert.equal(result.idle,true);
  assert.deepEqual(calls,[`eth_getBlockByNumber:${base}`],'idle reconciliation checks only the checkpoint anchor');
}

function rpc({logs=[],orphan=false}={}){
  const calls=[];
  return {calls,client:{
    async getBlockNumber(){calls.push('eth_blockNumber');return 41_799_000n;},
    async getBlock({blockNumber}){
      const n=Number(blockNumber);calls.push(`eth_getBlockByNumber:${n}`);
      if(orphan&&n===base+75)return block(n,H(n+900_000));
      if(orphan&&n===base+100)return block(n,H(n+900_000));
      return block(n);
    },
    async getLogs(args){calls.push(`eth_getLogs:${Number(args.fromBlock)}-${Number(args.toBlock)}`);return logs;},
    async getTransactionReceipt({hash}){
      calls.push('eth_getTransactionReceipt');return {status:'success',transactionHash:hash,blockNumber:BigInt(base+25),
        blockHash:H(base+25),transactionIndex:0,logs};
    },
    async getChainId(){return 4663;},
    async getBytecode(){return '0x01';},
    async readContract(){return 18;},
  }};
}

// HyperSync half-open ranges, data[].logs shape, exact cursor pagination, and
// rollback_guard continuity are validated before a batch can reach the writer.
{
  const bodies=[];let page=0;
  const rawLog=n=>({address:manifest.marketplace.toLowerCase(),block_number:n,block_hash:H(n),transaction_hash:H(n+5000),transaction_index:0,log_index:0,
    topic0:marketEvents.getEvent('PlatformFeeInfoUpdated').topicHash,topic1:H(1),data:'0x',removed:false});
  const rawBlock=n=>({number:n,hash:H(n),parent_hash:H(n-1)});
  let readySignals=0;
  const source=new HyperSyncLogSource({apiToken:'test-token',scheduler:testScheduler().scheduler,onReady:()=>readySignals++,fetchImpl:async(url,options)=>{
    const body=JSON.parse(options.body);bodies.push(body);page++;
    const response=page===1?{data:[{logs:[rawLog(100)],blocks:[rawBlock(100),rawBlock(101)]}],next_block:102,rollback_guard:{block_number:101,hash:H(101),first_block_number:100,first_parent_hash:H(99)}}:
      {data:[{logs:[rawLog(102),rawLog(103)],blocks:[rawBlock(102),rawBlock(103)]}],next_block:104,rollback_guard:{block_number:103,hash:H(103),first_block_number:102,first_parent_hash:H(101)}};
    return {ok:true,json:async()=>response};
  }});
  const result=await source.getLogs({address:manifest.marketplace,fromBlock:100,toBlock:104});
  assert.equal(result.logs.length,3,'multi-page ranges retain every event');
  assert.deepEqual(result.logs.map(l=>l.blockNumber),[100,102,103]);
  assert.equal(result.headers.length,4,'all headers are checked transiently for continuity');
  assert.deepEqual(bodies.map(b=>b.from_block),[100,102],'next_block resumes without gaps or overlap');
  assert.deepEqual(bodies.map(b=>b.to_block),[104,104]);
  assert.deepEqual(bodies[0].logs,[{address:[manifest.marketplace.toLowerCase()]}]);
  assert.equal(bodies[0].include_all_blocks,true);
  assert.deepEqual(bodies[0].field_selection.block,['number','hash','parent_hash']);
  assert.ok(bodies[0].field_selection.log.includes('transaction_index'));
  assert.ok(bodies[0].field_selection.log.includes('block_hash'));
  assert.equal(readySignals,1,'ENVIO_READY can only follow one complete validated response');
}
{
  const source=new HyperSyncLogSource({apiToken:'test-token',scheduler:testScheduler().scheduler,fetchImpl:async()=>({ok:true,json:async()=>({data:[{blocks:[{number:500,hash:H(500),parent_hash:H(499)}]}],next_block:501,rollback_guard:null})})});
  const result=await source.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501});
  assert.deepEqual(result.logs,[],'empty ranges still return complete checked range');
  assert.equal(result.rollbackGuards.length,0);
  const bad=new HyperSyncLogSource({apiToken:'test-token',scheduler:testScheduler().scheduler,fetchImpl:async()=>({ok:true,json:async()=>({data:[],next_block:500})})});
  await assert.rejects(bad.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501}),/HYPERSYNC_CURSOR_INVALID/);
  await assert.rejects(bad.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501}),error=>error.code==='HYPERSYNC_CURSOR_INVALID'&&error.fromBlock===500&&error.toBlock===501&&error.nextBlock===500&&error.httpStatus===undefined);
  const missingCursor=new HyperSyncLogSource({apiToken:'test-token',scheduler:testScheduler().scheduler,fetchImpl:async()=>({ok:true,status:200,json:async()=>({data:[]})})});
  await assert.rejects(missingCursor.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501}),error=>error.code==='HYPERSYNC_CURSOR_INVALID'&&error.fromBlock===500&&error.toBlock===501&&error.httpStatus===200);
  let readErrorBody=false;
  let rejectedAttempts=0;
  const throttleClock=testScheduler();
  const throttled=new HyperSyncLogSource({apiToken:'test-token',scheduler:throttleClock.scheduler,maxRetries:2,fetchImpl:async()=>{rejectedAttempts++;return {ok:false,status:429,headers:{get:name=>name==='retry-after'?'30':null},text:async()=>{readErrorBody=true;return 'private response body';}};}});
  await assert.rejects(throttled.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501}),error=>error.code==='HYPERSYNC_HTTP_429'&&error.httpStatus===429&&error.retryAfterMs===30000);
  assert.equal(readErrorBody,false,'HTTP diagnostics do not read or expose response bodies');
  assert.equal(rejectedAttempts,3,'each 429 retry is a separately counted bounded transport attempt');
  assert.ok(throttleClock.now()>=60000,'Retry-After delays are respected before retries');
  const serverFailure=new HyperSyncLogSource({apiToken:'test-token',scheduler:testScheduler().scheduler,maxRetries:0,fetchImpl:async()=>({ok:false,status:503,headers:{get:()=>null}})});
  await assert.rejects(serverFailure.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501}),error=>error.code==='HYPERSYNC_HTTP_SERVER'&&error.httpStatus===503);
  let page=0;
  const forked=new HyperSyncLogSource({apiToken:'test-token',scheduler:testScheduler().scheduler,fetchImpl:async()=>({ok:true,json:async()=>{
    page++;
    return page===1?{data:[{blocks:[{number:1,hash:H(1),parent_hash:H(0)}]}],next_block:2}:
      {data:[{blocks:[{number:2,hash:H(2),parent_hash:H(900)}]}],next_block:3};
  }})});
  await assert.rejects(forked.getLogs({address:manifest.marketplace,fromBlock:1,toBlock:3}),/HYPERSYNC_REORG_DETECTED/);
}

// The worker-wide scheduler serializes parallel callers and enforces a strict
// rolling window rather than a per-source or per-reconciliation allowance.
{
  const {scheduler,now}=testScheduler();let sent=0;
  await Promise.all(Array.from({length:11},()=>scheduler.request(async()=>{sent++;})));
  assert.equal(sent,11);
  assert.ok(now()>=60000,'the eleventh concurrent data request waits for the rolling window');
}

// A request that is rejected once is retried only after Retry-After, and the
// successful response resumes normal cursor coverage.
{
  const clock=testScheduler();let sent=0;
  const source=new HyperSyncLogSource({apiToken:'test-token',scheduler:clock.scheduler,maxRetries:2,fetchImpl:async()=>{
    sent++;
    if(sent===1)return {ok:false,status:429,headers:{get:name=>name==='retry-after'?'7':null}};
    return {ok:true,status:200,json:async()=>({data:[{blocks:[{number:700,hash:H(700),parent_hash:H(699)}]}],next_block:701,rollback_guard:null})};
  }});
  const recovered=await source.getLogs({address:manifest.marketplace,fromBlock:700,toBlock:701});
  assert.equal(sent,2,'one bounded retry recovers the data page');
  assert.ok(clock.now()>=7000,'429 recovery honors Retry-After');
  assert.equal(recovered.headers.length,1,'the recovered page is still fully validated');
}

// Restart recovery does not depend on having retained WebSocket notifications:
// it starts from the database checkpoint and scans the whole confirmed gap.
{
  const eventBlock=base+25,tx=H(77),fragment=marketEvents.getEvent('PlatformFeeInfoUpdated');
  const encoded=marketEvents.encodeEventLog(fragment,['0x0000000000000000000000000000000000000001',250]);
  const event={address:manifest.marketplace,blockNumber:BigInt(eventBlock),blockHash:H(eventBlock),transactionHash:tx,transactionIndex:0,logIndex:0,topics:encoded.topics,data:encoded.data,removed:false};
  const {client,calls}=rpc({logs:[event]});let committed=null,queryRange=null;
  const writer={manifest,async checkpoint(){return checkpoint;},async commit(batch){committed=batch;return {blockNumber:batch.range.toBlock};}};
  const result=await indexOnce({client,writer,confirmations:12,batchSize:100,checkpoint,head:BigInt(base+100),verifyContracts:false,
    logSource:{async getLogs(range){queryRange=range;return {logs:[event],headers:Array.from({length:88},(_,i)=>({number:base+1+i,hash:H(base+1+i),parentHash:H(base+i)})),rollbackGuards:[]};}}});
  assert.equal(result.blockNumber,base+88);
  assert.deepEqual(queryRange,{address:manifest.marketplace,fromBlock:base+1,toBlock:base+89});
  assert.equal(committed.events.length,1,'restart scan recovers a Marketplace event with no WebSocket hint');
  assert.ok(calls.includes('eth_getTransactionReceipt'),'recovered event still receives Alchemy receipt verification');
}

// Empty, notification-independent recovery still queries the entire bounded range,
// and journals only the checked endpoint.
{
  const {client,calls}=rpc();
  const batch=await readBatch({client,manifest,checkpoint,toBlock:base+20,head:41_799_000n,
    anchor:block(base),verifyContracts:false,logSource:{async getLogs(q){
      assert.equal(q.fromBlock,base+1);assert.equal(q.toBlock,base+21);
      return {logs:[],headers:Array.from({length:20},(_,i)=>({number:base+1+i,hash:H(base+1+i),parentHash:H(base+i)})),rollbackGuards:[]};
    }}});
  assert.deepEqual(batch.blocks.map(b=>b.number),[base+20]);
  assert.equal(batch.range.fromBlock,base+1);
  assert.equal(batch.range.toBlock,base+20);
  assert.ok(!calls.some(c=>c.startsWith('eth_getLogs:')),'historical range uses HyperSync rather than Alchemy eth_getLogs');
  assert.equal(calls.filter(c=>c.startsWith('eth_getBlockByNumber:')).length,1,
    'empty range reads one canonical endpoint after the parent-linked HyperSync scan');
}

// A matching proxy event retains its canonical block row and receipt evidence.
{
  const eventBlock=base+25,tx=H(88),fragment=marketEvents.getEvent('PlatformFeeInfoUpdated');
  const encoded=marketEvents.encodeEventLog(fragment,['0x0000000000000000000000000000000000000001',250]);
  const log={address:manifest.marketplace,blockNumber:BigInt(eventBlock),blockHash:H(eventBlock),transactionHash:tx,
    transactionIndex:0,logIndex:0,topics:encoded.topics,data:encoded.data,removed:false};
  const {client,calls}=rpc({logs:[log]});
  const batch=await readBatch({client,manifest,checkpoint,toBlock:base+30,head:41_799_000n,
    anchor:block(base),verifyContracts:false,logSource:{async getLogs(q){
      assert.equal(q.address,manifest.marketplace);
      assert.equal(q.fromBlock,base+1);
      assert.equal(q.toBlock,base+31,'HyperSync uses an exclusive upper bound');
      return {logs:[log],headers:Array.from({length:30},(_,i)=>({number:base+1+i,hash:H(base+1+i),parentHash:H(base+i)})),rollbackGuards:[]};
    }}});
  assert.deepEqual(batch.blocks.map(b=>b.number),[eventBlock,base+30]);
  assert.equal(batch.events.length,1);
  assert.equal(batch.events[0].blockHash,H(eventBlock));
  assert.ok(!calls.some(c=>c.startsWith('eth_getLogs:')),'verified HyperSync path replaces Alchemy log queries');
}

// If the last block contains an event, its receipt timestamp remains sourced
// from the canonical RPC header while the range endpoint is checked after receipts.
{
  const eventBlock=base+25,tx=H(89),fragment=marketEvents.getEvent('PlatformFeeInfoUpdated');
  const encoded=marketEvents.encodeEventLog(fragment,['0x0000000000000000000000000000000000000001',250]);
  const log={address:manifest.marketplace,blockNumber:BigInt(eventBlock),blockHash:H(eventBlock),transactionHash:tx,transactionIndex:0,logIndex:0,topics:encoded.topics,data:encoded.data,removed:false};
  const {client,calls}=rpc({logs:[log]});
  const batch=await readBatch({client,manifest,checkpoint,toBlock:eventBlock,head:41_799_000n,anchor:block(base),verifyContracts:false,
    logSource:{async getLogs(){return {logs:[log],headers:Array.from({length:eventBlock-base},(_,i)=>({number:base+1+i,hash:H(base+1+i),parentHash:H(base+i)})),rollbackGuards:[]};}}});
  assert.equal(batch.events[0].blockTimestamp,new Date(Number(block(eventBlock).timestamp)*1000).toISOString());
  assert.equal(calls.filter(c=>c.startsWith('eth_getBlockByNumber:')).length,2,'event block metadata and final canonical endpoint are independently checked');
}

// Writer accepts an empty checked range and records its endpoint transactionally.
{
  const inserted=[];
  const db={async query(sql,values=[]){
    if(sql==='BEGIN'||sql==='COMMIT'||sql==='ROLLBACK')return {rows:[]};
    if(sql.includes('SELECT * FROM relic_market_indexer.checkpoints'))return {rows:[checkpoint]};
    if(sql.includes('FROM public.relic_market_contracts'))return {rows:[
      {role:'MARKETPLACE_V3',contract_address:manifest.marketplace,start_block:base},
      {role:'PRINTER',contract_address:manifest.printer,start_block:base},
      {role:'RELIC_NFT',contract_address:manifest.nft,start_block:base},
    ]};
    if(sql.includes('INSERT INTO relic_market_indexer.blocks')){inserted.push(values[1]);return {rows:[]};}
    if(sql.includes('INSERT INTO relic_market_indexer.wallet_volume_state'))return {rows:[{stream:'relic-mainnet'}]};
    if(sql.includes('UPDATE relic_market_indexer.checkpoints'))return {rows:[{version:'8'}]};
    throw Error(`Unexpected SQL in empty-range test: ${sql}`);
  },release(){}};
  const writer=new PostgresMarketplaceWriter({pool:{connect:async()=>db},stream:'relic-mainnet',manifest});
  const result=await writer.commit({expectedVersion:'7',range:{fromBlock:base+1,toBlock:base+20},
    blocks:[{number:base+20,hash:H(base+20),parentHash:H(base+19)}],events:[]});
  assert.equal(result.blockNumber,base+20);
  assert.deepEqual(inserted,[base+20]);
  await assert.rejects(writer.advanceCheckpoint({blockNumber:base+21,blockHash:H(base+21),expectedVersion:'7'}),
    /Indexer transaction failed/);
}

// Historical replay uses the normal projection path below the live checkpoint,
// but never changes the live checkpoint or its volume watermark.
{
  const eventBlock=base+10,tx=H(901),fragment=marketEvents.getEvent('PlatformFeeInfoUpdated');
  const encoded=marketEvents.encodeEventLog(fragment,['0x0000000000000000000000000000000000000001',250]);
  const event={address:manifest.marketplace,blockNumber:eventBlock,blockHash:H(eventBlock),transactionHash:tx,
    transactionIndex:0,logIndex:0,topics:encoded.topics,data:encoded.data,removed:false,blockTimestamp:null};
  const storedBlocks=new Map(),sql=[];
  const db={async query(text,values=[]){sql.push(text);
    if(text==='BEGIN'||text==='COMMIT'||text==='ROLLBACK')return {rows:[]};
    if(text.includes('SELECT * FROM relic_market_indexer.checkpoints'))return {rows:[{...checkpoint,block_number:base+100,block_hash:H(base+100)}]};
    if(text.includes('FROM public.relic_market_contracts'))return {rows:[
      {role:'MARKETPLACE_V3',contract_address:manifest.marketplace,start_block:base},
      {role:'PRINTER',contract_address:manifest.printer,start_block:base},
      {role:'RELIC_NFT',contract_address:manifest.nft,start_block:base},
    ]};
    if(text.includes('INSERT INTO relic_market_indexer.blocks')){storedBlocks.set(values[1],{block_hash:values[2],parent_hash:values[3]});return {rows:[]};}
    if(text.includes('SELECT block_hash,parent_hash FROM relic_market_indexer.blocks'))return {rows:[storedBlocks.get(values[1])]};
    if(text.includes('SELECT max(block_number) AS block_number'))return {rows:[{block_number:null}]};
    if(text.includes('SELECT count(*)::text AS count FROM relic_market_indexer.events'))return {rows:[{count:'0'}]};
    if(text.includes('INSERT INTO public.relic_market_raw_events'))return {rows:[{chain_id:4663}]};
    if(text.includes('INSERT INTO relic_market_indexer.events'))return {rows:[]};
    if(text.includes('INSERT INTO public.relic_market_activity'))return {rows:[{chain_id:4663}]};
    if(text.includes('INSERT INTO public.relic_market_config_events'))return {rows:[{chain_id:4663}]};
    throw Error(`Unexpected SQL in historical replay test: ${text}`);
  },release(){}};
  const writer=new PostgresMarketplaceWriter({pool:{connect:async()=>db},stream:'relic-mainnet',manifest});
  const result=await writer.replayHistorical({expectedVersion:'7',range:{fromBlock:base+1,toBlock:base+20},
    blocks:[{number:eventBlock,hash:H(eventBlock),parentHash:H(eventBlock-1)},{number:base+20,hash:H(base+20),parentHash:H(base+19)}],events:[event]});
  assert.equal(result.unchangedCheckpoint,true);
  assert.equal(result.blockNumber,base+100);
  assert.equal(result.replayedEvents,1);
  assert.ok(sql.some(text=>text.includes('entity_undo'))===false,'configuration-only history requires no entity undo rows');
  assert.ok(!sql.some(text=>text.includes('UPDATE relic_market_indexer.checkpoints')),'historical replay never rewinds or advances checkpoint');
  await assert.rejects(writer.replayHistorical({expectedVersion:'7',range:{fromBlock:base+1,toBlock:base+101},blocks:[{number:base+101,hash:H(base+101),parentHash:H(base+100)}],events:[]}),/Indexer transaction failed/);
}

// Reorg rollback falls back from orphan event/checkpoint rows to a canonical sparse boundary.
{
  const orphanBlock=base+75,orphanHash=H(orphanBlock),eventTx=H(89);
  const fragment=marketEvents.getEvent('PlatformFeeInfoUpdated');
  const encoded=marketEvents.encodeEventLog(fragment,['0x0000000000000000000000000000000000000001',250]);
  const raw={contract_address:manifest.marketplace,transaction_hash:eventTx,log_index:0,block_number:orphanBlock,
    block_hash:orphanHash,transaction_index:0,block_timestamp:null,topics:encoded.topics,data:encoded.data,
    removed:false,ingest_source:'LIVE',journal_block_number:orphanBlock,journal_block_hash:orphanHash};
  const undo=[],rollbackSql=[];
  const db={async query(sql){rollbackSql.push(sql);
    if(sql==='BEGIN'||sql==='COMMIT'||sql==='ROLLBACK')return {rows:[]};
    if(sql.includes('SELECT * FROM relic_market_indexer.checkpoints'))return {rows:[{...checkpoint,
      block_number:base+100,block_hash:H(base+100)}]};
    if(sql.includes('FROM relic_market_indexer.blocks WHERE stream=$1 ORDER BY block_number DESC'))return {rows:[
      {block_number:base+100,block_hash:H(base+100)},
      {block_number:orphanBlock,block_hash:orphanHash},
      {block_number:base+50,block_hash:H(base+50)},
    ]};
    if(sql.includes('FROM public.relic_market_contracts'))return {rows:[
      {role:'MARKETPLACE_V3',contract_address:manifest.marketplace,start_block:base},
      {role:'PRINTER',contract_address:manifest.printer,start_block:base},
      {role:'RELIC_NFT',contract_address:manifest.nft,start_block:base},
    ]};
    if(sql.includes('FROM relic_market_indexer.events j JOIN relic_market_indexer.blocks'))return {rows:[raw]};
    if(sql.includes('FROM relic_market_indexer.entity_undo')){undo.push('restore');return {rows:[]};}
    if(sql.includes('INSERT INTO relic_market_indexer.wallet_volume_state'))return {rows:[{stream:'relic-mainnet'}]};
    if(sql.includes('DELETE FROM public.relic_market_raw_events'))return {rows:[{chain_id:4663}]};
    if(sql.includes('UPDATE relic_market_indexer.checkpoints'))return {rows:[{version:'8'}]};
    return {rows:[]};
  },release(){}};
  const writer=new PostgresMarketplaceWriter({pool:{connect:async()=>db},stream:'relic-mainnet',manifest});
  const result=await writer.recover({client:rpc({orphan:true}).client,expectedVersion:'7'});
  assert.equal(result.blockNumber,base+50);
  assert.deepEqual(undo,['restore']);
  assert.ok(rollbackSql.some(sql=>sql.includes('DELETE FROM public.relic_market_settlements')),'orphan settlements are removed with the undo rollback');
  assert.ok(rollbackSql.some(sql=>sql.includes('DELETE FROM relic_market_indexer.blocks')),'sparse and legacy dense descendants are removed atomically');
}

console.log('Marketplace indexer focused checks passed: historical transitions, receipt settlements, HyperRPC fallback, replay checkpoint safety, empty ranges, bounded recovery and reorg rollback.');
