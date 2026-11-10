import assert from 'node:assert/strict';
import {Interface,keccak256} from 'ethers';
import {PRODUCTION_CONFIG} from '../shared/production-config.mjs';
import {marketEvents} from '../indexer/marketplace.mjs';
import {TARGETED_TRANSACTIONS,verifyTargetedReceipt,targetedReplayBatches,verifyTargetedProjection} from './audit-marketplace-history.mjs';
import {expectedSale} from './replay-marketplace-history.mjs';

const manifest={chainId:4663,nft:PRODUCTION_CONFIG.nftAddress,printer:PRODUCTION_CONFIG.currencyAddress,
  marketplace:'0xb87bbab928495bec8085eca758660790a0c67f9f',printerDecimals:18,
  codeHashes:{nft:keccak256('0x01'),printer:keccak256('0x02'),marketplace:keccak256('0x03')},feeRecipients:[
    {address:'0x87cee0881b121f3557e4b319d35d75dedbfebe2b',role:'platform'},
    {address:'0x1af20c6b23373350ad464700b5965ce4b0d2ad94',role:'thirdweb'},
  ]};
const tx=TARGETED_TRANSACTIONS[0],blockNumber=tx.block,blockHash=`0x${'a'.repeat(64)}`,parentHash=`0x${'b'.repeat(64)}`;
const fragment=marketEvents.getEvent('NewListing'),creator='0x591f30b91573a072be38c0c6e42a04a48eab817b';
const tuple={listingId:'7',tokenId:'9',quantity:'1',pricePerToken:'100',startTimestamp:'1700000000',endTimestamp:((1n<<128n)-1n).toString(),listingCreator:creator,assetContract:manifest.nft,currency:manifest.printer,tokenType:'0',status:'1',reserved:false};
const encoded=marketEvents.encodeEventLog(fragment,[creator,'7',manifest.nft,tuple]);
const baseLog={address:manifest.marketplace,blockNumber,blockHash,transactionHash:tx.hash,transactionIndex:0,logIndex:0,topics:encoded.topics,data:encoded.data,removed:false};
const block={number:BigInt(blockNumber),hash:blockHash,parentHash,timestamp:1_800_000_000n};
const receipt={status:'success',transactionHash:tx.hash,blockNumber:BigInt(blockNumber),blockHash,transactionIndex:0n,logs:[baseLog]};
const cp={version:'5'};

assert.equal(TARGETED_TRANSACTIONS.length,11);
assert.ok(TARGETED_TRANSACTIONS.every(x=>/^0x[0-9a-f]{64}$/.test(x.hash)),'all embedded receipt hashes are canonical 32-byte hashes');
assert.equal(TARGETED_TRANSACTIONS.find(x=>x.block===100809473).hash,'0xd8da80961e134a5231cd91d54d4d4d759caa8056bb128cca791e32d8a6992c10','the malformed 31-byte value is corrected from the block-filtered RPC result');
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

const buyer='0x2222222222222222222222222222222222222222';
const erc20=new Interface(['event Transfer(address indexed from,address indexed to,uint256 value)']);
const printerTransfer=(to,value,index)=>{const e=erc20.encodeEventLog('Transfer',[buyer,to,value]);return {...baseLog,address:manifest.printer,transactionHash:saleTx,logIndex:index,topics:e.topics,data:e.data};};
const validNft={...nftLog,topics:nftEncoded.topics};
const payoutLogs=[printerTransfer(creator,'8550000000000000000',3),printerTransfer(manifest.feeRecipients[0].address,'405000000000000000',4),printerTransfer(manifest.feeRecipients[1].address,'45000000000000000',5)];
const fullSale={...saleLog,data:marketEvents.encodeEventLog(saleFragment,[creator,'7',manifest.nft,'9',buyer,'1','9000000000000000000']).data,topics:marketEvents.encodeEventLog(saleFragment,[creator,'7',manifest.nft,'9',buyer,'1','9000000000000000000']).topics};
const fullListingTuple={...tuple,pricePerToken:'9000000000000000000'};
const fullListingEncoded=marketEvents.encodeEventLog(fragment,[creator,'7',manifest.nft,fullListingTuple]);
const fullListing={...listingLog,data:fullListingEncoded.data,topics:fullListingEncoded.topics};
const reconciled=verifyTargetedProjection([fullListing,fullSale,validNft,...payoutLogs],manifest);
assert.equal(reconciled.ambiguous.length,0,'sale with exact NFT and PRINTER settlement evidence reconciles');
assert.equal(reconciled.rows.relic_market_settlements[0].reconcile_status,'RECONCILED');
assert.equal(reconciled.rows.relic_market_settlements[0].gross_amount_raw,'9000000000000000000');
assert.equal(reconciled.rows.relic_market_settlements[0].seller_proceeds_raw,'8550000000000000000');
assert.equal(reconciled.rows.relic_market_settlements[0].platform_fee_raw,'405000000000000000');
assert.equal(reconciled.rows.relic_market_settlements[0].thirdweb_fee_raw,'45000000000000000');
assert.equal(verifyTargetedProjection([fullListing,fullSale,validNft,...payoutLogs.slice(0,2)],manifest).ambiguous.length,1,'missing Thirdweb transfer is rejected');
assert.equal(verifyTargetedProjection([fullListing,fullSale,validNft,payoutLogs[0],payoutLogs[1],printerTransfer(manifest.feeRecipients[1].address,'44000000000000000',5)],manifest).ambiguous.length,1,'incorrect Thirdweb amount is rejected');
assert.equal(verifyTargetedProjection([fullListing,fullSale,validNft,printerTransfer(creator,'8540000000000000000',3),...payoutLogs.slice(1)],manifest).ambiguous.length,1,'incorrect seller payout is rejected');
assert.equal(verifyTargetedProjection([fullListing,fullSale,validNft,...payoutLogs,printerTransfer('0x4444444444444444444444444444444444444444','10000000000000000',6)],manifest).ambiguous.length,1,'unknown payout recipient is rejected');
const unknown='0x4444444444444444444444444444444444444444';
assert.equal(verifyTargetedProjection([fullListing,fullSale,validNft,...payoutLogs,printerTransfer(unknown,'10000000000000000',6),printerTransfer(buyer,'10000000000000000',7)],manifest).ambiguous.length,1,'unknown recipient cannot be hidden by a zero net balance');
const secondListingTuple={...fullListingTuple,listingId:'8',tokenId:'10'};
const secondListingEncoded=marketEvents.encodeEventLog(fragment,[creator,'8',manifest.nft,secondListingTuple]);
const secondListing={...fullListing,logIndex:6,topics:secondListingEncoded.topics,data:secondListingEncoded.data};
const secondSaleEncoded=marketEvents.encodeEventLog(saleFragment,[creator,'8',manifest.nft,'10',buyer,'1','9000000000000000000']);
const secondSale={...fullSale,logIndex:7,topics:secondSaleEncoded.topics,data:secondSaleEncoded.data};
assert.equal(verifyTargetedProjection([fullListing,fullSale,validNft,...payoutLogs,secondListing,secondSale],manifest).ambiguous.length,2,'multiple sales in one transaction remain ambiguous');
const knownTx='0x43d9b857246f88429a9e24d1e1d4b9dd620e0b69bd0c61255cfca57b5298699a';
const knownExpected={settlement:{reconcile_status:'RECONCILED',listing_id:'2',nft_contract:manifest.nft,token_id:'1509',buyer_address:'0x1ffee3db7d6135595e2d50af515b5f137751f5c9',seller_address:'0x591f30b91573a072be38c0c6e42a04a48eab817b',currency_address:manifest.printer,currency_decimals:'18',gross_amount_raw:'9000000000000000000',seller_proceeds_raw:'8550000000000000000',platform_fee_raw:'405000000000000000',thirdweb_fee_raw:'45000000000000000'},
  payments:[{token_contract:manifest.printer,from_address:'0x1ffee3db7d6135595e2d50af515b5f137751f5c9',to_address:'0x591f30b91573a072be38c0c6e42a04a48eab817b',amount_raw:'8550000000000000000',payment_role:'SELLER'},
    {token_contract:manifest.printer,from_address:'0x1ffee3db7d6135595e2d50af515b5f137751f5c9',to_address:manifest.feeRecipients[0].address,amount_raw:'405000000000000000',payment_role:'PLATFORM'},
    {token_contract:manifest.printer,from_address:'0x1ffee3db7d6135595e2d50af515b5f137751f5c9',to_address:manifest.feeRecipients[1].address,amount_raw:'45000000000000000',payment_role:'THIRDWEB'}],
  nft:[{log_index:4,from_address:'0x591f30b91573a072be38c0c6e42a04a48eab817b',to_address:'0x1ffee3db7d6135595e2d50af515b5f137751f5c9',token_id:'1509'}]};
assert.equal(expectedSale({financialValidation:{sales:[{transactionHash:knownTx,logIndex:4,expected:knownExpected}]}}).expected.settlement.reconcile_status,'RECONCILED');
const badExpected=structuredClone(knownExpected);badExpected.settlement.thirdweb_fee_raw='44000000000000000';
assert.throws(()=>expectedSale({financialValidation:{sales:[{transactionHash:knownTx,logIndex:4,expected:badExpected}]}}),/KNOWN_SALE_EXPECTED_SETTLEMENT_MISMATCH/);

process.stdout.write('targeted marketplace recovery tests passed\n');
