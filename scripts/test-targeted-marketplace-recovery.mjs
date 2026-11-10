import assert from 'node:assert/strict';
import {Interface,keccak256} from 'ethers';
import {PRODUCTION_CONFIG} from '../shared/production-config.mjs';
import {marketEvents} from '../indexer/marketplace.mjs';
import {TARGETED_TRANSACTIONS,verifyTargetedReceipt,targetedReplayBatches,verifyTargetedProjection} from './audit-marketplace-history.mjs';

const manifest={chainId:4663,nft:PRODUCTION_CONFIG.nftAddress,printer:PRODUCTION_CONFIG.currencyAddress,
  marketplace:'0xb87bbab928495bec8085eca758660790a0c67f9f',printerDecimals:18,
  codeHashes:{nft:keccak256('0x01'),printer:keccak256('0x02'),marketplace:keccak256('0x03')},feeRecipients:[]};
const tx=TARGETED_TRANSACTIONS[0],blockNumber=tx.block,blockHash=`0x${'a'.repeat(64)}`,parentHash=`0x${'b'.repeat(64)}`;
const fragment=marketEvents.getEvent('NewListing'),creator='0x591f30b91573a072be38c0c6e42a04a48eab817b';
const tuple={listingId:'7',tokenId:'9',quantity:'1',pricePerToken:'100',startTimestamp:'1700000000',endTimestamp:((1n<<128n)-1n).toString(),listingCreator:creator,assetContract:manifest.nft,currency:manifest.printer,tokenType:'0',status:'1',reserved:false};
const encoded=marketEvents.encodeEventLog(fragment,[creator,'7',manifest.nft,tuple]);
const baseLog={address:manifest.marketplace,blockNumber,blockHash,transactionHash:tx.hash,transactionIndex:0,logIndex:0,topics:encoded.topics,data:encoded.data,removed:false};
const block={number:BigInt(blockNumber),hash:blockHash,parentHash,timestamp:1_800_000_000n};
const receipt={status:'success',transactionHash:tx.hash,blockNumber:BigInt(blockNumber),blockHash,transactionIndex:0n,logs:[baseLog]};
const cp={version:'5'};

assert.equal(TARGETED_TRANSACTIONS.length,11);
assert.deepEqual(verifyTargetedReceipt({expected:tx,receipt,canonicalBlock:block,head:BigInt(blockNumber+12),manifest}).length,1);
assert.throws(()=>verifyTargetedReceipt({expected:tx,receipt:null,canonicalBlock:block,head:BigInt(blockNumber+12),manifest}),/TARGETED_RECEIPT_MISSING/);
assert.throws(()=>verifyTargetedReceipt({expected:tx,receipt:{...receipt,status:'reverted'},canonicalBlock:block,head:BigInt(blockNumber+12),manifest}),/TARGETED_RECEIPT_FAILED/);
assert.throws(()=>verifyTargetedReceipt({expected:tx,receipt,canonicalBlock:{...block,hash:parentHash},head:BigInt(blockNumber+12),manifest}),/TARGETED_CANONICAL_HASH_MISMATCH/);
assert.throws(()=>verifyTargetedReceipt({expected:tx,receipt,canonicalBlock:block,head:BigInt(blockNumber+11),manifest}),/TARGETED_CONFIRMATIONS_INSUFFICIENT/);
assert.throws(()=>verifyTargetedReceipt({expected:tx,receipt:{...receipt,logs:[baseLog,{...baseLog}]},canonicalBlock:block,head:BigInt(blockNumber+12),manifest}),/TARGETED_DUPLICATE_LOG_INDEX/);

const replayInput=[{block,logs:[baseLog,{...baseLog}]}];
assert.equal(targetedReplayBatches(replayInput,cp,manifest)[0].events.length,1,'identical duplicate receipt log is idempotently deduplicated');
assert.deepEqual(targetedReplayBatches(replayInput,cp,manifest),targetedReplayBatches(replayInput,cp,manifest),'replay serialization is deterministic');
assert.throws(()=>targetedReplayBatches([{block,logs:[baseLog,{...baseLog,data:'0x'}]}],cp,manifest),/TARGETED_DUPLICATE_CONFLICT/);
const nextBlock={...block,number:BigInt(blockNumber+1),hash:`0x${'d'.repeat(64)}`,parentHash:blockHash};
const nextLog={...baseLog,blockNumber:blockNumber+1,blockHash:nextBlock.hash,transactionHash:`0x${'e'.repeat(64)}`};
assert.deepEqual(targetedReplayBatches([{block:nextBlock,logs:[nextLog]},{block,logs:[baseLog]}],cp,manifest).map(x=>x.range.fromBlock),[blockNumber,blockNumber+1],'replay input is ascending even when fetched receipts are supplied out of order');

// An incomplete/misrouted sale transfer set must remain ambiguous under the
// existing projection; the targeted audit does not mark it replay-ready.
const saleTx=`0x${'c'.repeat(64)}`;
const listingLog={...baseLog,transactionHash:saleTx,logIndex:0,topics:marketEvents.encodeEventLog(fragment,[creator,'7',manifest.nft,tuple]).topics,data:marketEvents.encodeEventLog(fragment,[creator,'7',manifest.nft,tuple]).data};
const saleFragment=marketEvents.getEvent('NewSale');
const saleEncoded=marketEvents.encodeEventLog(saleFragment,[creator,'7',manifest.nft,'9','0x2222222222222222222222222222222222222222','1','100']);
const saleLog={...baseLog,transactionHash:saleTx,logIndex:1,topics:saleEncoded.topics,data:saleEncoded.data};
const transferIface=new Interface(['event Transfer(address indexed from,address indexed to,uint256 indexed tokenId)']);
const nftEncoded=transferIface.encodeEventLog('Transfer',[creator,'0x2222222222222222222222222222222222222222','9']);
const nftLog={...baseLog,address:manifest.nft,transactionHash:saleTx,logIndex:2,topics:nftEncoded.topics,data:nftEncoded.data};
const incomplete=verifyTargetedProjection([listingLog,saleLog,nftLog],manifest);
assert.equal(incomplete.ambiguous.length,1,'sale without PRINTER receipt transfers is not reconciled');
assert.equal(incomplete.rows.relic_market_settlements[0].reconcile_status,'AMBIGUOUS');
const badNft={...nftLog,topics:transferIface.encodeEventLog('Transfer',[creator,'0x3333333333333333333333333333333333333333','9']).topics};
assert.equal(verifyTargetedProjection([listingLog,saleLog,badNft],manifest).ambiguous.length,1,'incorrect NFT recipient is not accepted as a verified sale');

process.stdout.write('targeted marketplace recovery tests passed\n');
