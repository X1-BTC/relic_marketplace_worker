import assert from 'node:assert/strict';
import {keccak256} from 'ethers';
import {PRODUCTION_CONFIG} from '../shared/production-config.mjs';
import {marketEvents,readBatch,manifestDigest} from '../indexer/marketplace.mjs';
import {PostgresMarketplaceWriter} from '../indexer/postgres-writer.mjs';
import {HyperSyncLogSource} from '../indexer/hypersync-client.mjs';
import {HyperSyncRequestScheduler} from '../indexer/hypersync-scheduler.mjs';
import {envioHyperRpcUrl,hybridReadClient} from '../indexer/hybrid-rpc.mjs';
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

// HyperRPC handles supported block and receipt methods; unsupported methods
// fall back once, while temporary provider failures fail closed to worker retry.
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
  const fallback=hybridReadClient({envio:make('envio',{getBlockNumber:unsupported}),alchemy:make('alchemy')});
  assert.equal(await fallback.getBlockNumber(),'alchemy:getBlockNumber');
  assert.equal(await fallback.getBlockNumber(),'alchemy:getBlockNumber');
  assert.equal(calls.envio.filter(name=>name==='getBlockNumber').length,2,'unsupported capability is cached after first failure');

  const transient=hybridReadClient({envio:make('envio',{getBlockNumber:new Error('temporary failure')}),alchemy:make('alchemy')});
  await assert.rejects(transient.getBlockNumber(),error=>error.code==='RPC_READ_FAILED');
  assert.equal(calls.alchemy.filter(name=>name==='getBlockNumber').length,2,'transient failure does not fall back to Alchemy');
  assert.equal(envioHyperRpcUrl('token/a'),'https://4663.rpc.hypersync.xyz/token%2Fa');
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

console.log('Sparse journal focused checks passed: empty ranges, event receipts, bounded recovery scan, and reorg rollback.');
