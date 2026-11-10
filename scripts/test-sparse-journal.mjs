import assert from 'node:assert/strict';
import {keccak256} from 'ethers';
import {PRODUCTION_CONFIG} from '../shared/production-config.mjs';
import {marketEvents,readBatch,manifestDigest} from '../indexer/marketplace.mjs';
import {PostgresMarketplaceWriter} from '../indexer/postgres-writer.mjs';
import {HyperSyncLogSource} from '../indexer/hypersync-client.mjs';
import {indexOnce} from '../indexer/run-once.mjs';

const H=n=>`0x${BigInt(n).toString(16).padStart(64,'0')}`;
const manifest={chainId:4663,nft:PRODUCTION_CONFIG.nftAddress,printer:PRODUCTION_CONFIG.currencyAddress,
  marketplace:'0xb87bbab928495bec8085eca758660790a0c67f9f',printerDecimals:18,
  codeHashes:{nft:keccak256('0x01'),printer:keccak256('0x01'),marketplace:keccak256('0x01')},feeRecipients:[]};
const base=41_798_500;
const checkpoint={chain_id:4663,manifest,manifest_digest:manifestDigest(manifest),anchor_number:base,
  anchor_hash:H(base),block_number:base,block_hash:H(base),version:'7'};
const block=(n,hash=H(n))=>({number:BigInt(n),hash,parentHash:H(n-1),timestamp:BigInt(1_700_000_000+n)});

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
  const source=new HyperSyncLogSource({apiToken:'test-token',fetchImpl:async(url,options)=>{
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
}
{
  const source=new HyperSyncLogSource({apiToken:'test-token',fetchImpl:async()=>({ok:true,json:async()=>({data:[{blocks:[{number:500,hash:H(500),parent_hash:H(499)}]}],next_block:501,rollback_guard:null})})});
  const result=await source.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501});
  assert.deepEqual(result.logs,[],'empty ranges still return complete checked range');
  assert.equal(result.rollbackGuards.length,0);
  const bad=new HyperSyncLogSource({apiToken:'test-token',fetchImpl:async()=>({ok:true,json:async()=>({data:[],next_block:500})})});
  await assert.rejects(bad.getLogs({address:manifest.marketplace,fromBlock:500,toBlock:501}),/HYPERSYNC_CURSOR_INVALID/);
  let page=0;
  const forked=new HyperSyncLogSource({apiToken:'test-token',fetchImpl:async()=>({ok:true,json:async()=>{
    page++;
    return page===1?{data:[{blocks:[{number:1,hash:H(1),parent_hash:H(0)}]}],next_block:2}:
      {data:[{blocks:[{number:2,hash:H(2),parent_hash:H(900)}]}],next_block:3};
  }})});
  await assert.rejects(forked.getLogs({address:manifest.marketplace,fromBlock:1,toBlock:3}),/HYPERSYNC_REORG_DETECTED/);
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
  assert.equal(calls.filter(c=>c.startsWith('eth_getBlockByNumber:')).length,3,
    'empty range reads endpoint before/after plus checkpoint boundary after scan');
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
