import { Interface, id, keccak256 } from 'ethers';
import { PRODUCTION_CONFIG } from '../shared/production-config.mjs';
import { address, tokenId } from '../shared/safety.mjs';

// Exact MarketplaceV3 IDirectListings / IOffers event tuples. No thirdweb runtime.
const listing = '(uint256 listingId,uint256 tokenId,uint256 quantity,uint256 pricePerToken,uint128 startTimestamp,uint128 endTimestamp,address listingCreator,address assetContract,address currency,uint8 tokenType,uint8 status,bool reserved)';
const offer = '(uint256 offerId,uint256 tokenId,uint256 quantity,uint256 totalPrice,uint256 expirationTimestamp,address offeror,address assetContract,address currency,uint8 tokenType,uint8 status)';
export const marketEvents = new Interface([
  `event NewListing(address indexed listingCreator,uint256 indexed listingId,address indexed assetContract,${listing} listing)`,
  `event UpdatedListing(address indexed listingCreator,uint256 indexed listingId,address indexed assetContract,${listing} listing)`,
  'event CancelledListing(address indexed listingCreator,uint256 indexed listingId)',
  'event NewSale(address indexed listingCreator,uint256 indexed listingId,address indexed assetContract,uint256 tokenId,address buyer,uint256 quantityBought,uint256 totalPricePaid)',
  `event NewOffer(address indexed offeror,uint256 indexed offerId,address indexed assetContract,${offer} offer)`,
  'event CancelledOffer(address indexed offeror,uint256 indexed offerId)',
  'event PlatformFeeInfoUpdated(address indexed platformFeeRecipient,uint256 platformFeeBps)',
  'event FlatPlatformFeeUpdated(address platformFeeRecipient,uint256 flatFee)',
  'event PlatformFeeTypeUpdated(uint8 feeType)',
  'event RoyaltyEngineUpdated(address indexed previousAddress,address indexed newAddress)',
  'event AcceptedOffer(address indexed offeror,uint256 indexed offerId,address indexed assetContract,uint256 tokenId,address seller,uint256 quantityBought,uint256 totalPricePaid)',
]);
const transfer = new Interface(['event Transfer(address indexed from,address indexed to,uint256 value)']);
const nftTransfer = new Interface(['event Transfer(address indexed from,address indexed to,uint256 indexed tokenId)']);
const hashPattern = /^0x[0-9a-f]{64}$/;
// PostgreSQL jsonb reorders object keys; hashes/comparisons must be canonical.
export const json = value => JSON.stringify(value, (_, v) => {
  if (typeof v === 'bigint') return v.toString();
  if (v && typeof v === 'object' && !Array.isArray(v)) return Object.fromEntries(Object.keys(v).sort().map(k=>[k,v[k]]));
  return v;
});
export const copy = value => JSON.parse(json(value));
export const addr = value => address(value).toLowerCase();
export function hash(value) { if (!hashPattern.test(value)) throw Error('Invalid hash'); return value; }
export function integer(value) { if (!Number.isSafeInteger(value) || value < 0) throw Error('Unsafe block/log number'); return value; }
export function validateManifest(m = {}) {
  if (m.chainId !== 4663) throw Error('Robinhood Mainnet 4663 required');
  if (addr(m.nft) !== PRODUCTION_CONFIG.nftAddress || addr(m.printer) !== PRODUCTION_CONFIG.currencyAddress) throw Error('Unauthorized NFT or PRINTER');
  if (!Number.isInteger(m.printerDecimals) || m.printerDecimals < 0 || m.printerDecimals > 255) throw Error('Attested PRINTER decimals required');
  addr(m.marketplace); // Unset production address deliberately fails closed.
  if (new Set([m.nft, m.printer, m.marketplace].map(addr)).size !== 3) throw Error('Distinct contracts required');
  for (const key of ['nft', 'printer', 'marketplace']) hash(m.codeHashes?.[key]);
  for (const r of m.feeRecipients ?? []) { addr(r.address); if (!['platform', 'thirdweb', 'royalty'].includes(r.role)) throw Error('Unknown fee role'); }
  if (new Set((m.feeRecipients ?? []).map(r => addr(r.address))).size !== (m.feeRecipients ?? []).length) throw Error('Duplicate fee recipient');
  return copy(m);
}
export const manifestDigest = m => id(json(validateManifest(m)));
export function normalizeLog(log) {
  if (log.removed) throw Error('Removed log; recover canonical checkpoint first');
  const result = {address: addr(log.address), blockNumber: integer(Number(log.blockNumber)), blockHash: hash(log.blockHash), transactionHash: hash(log.transactionHash), logIndex: integer(log.logIndex), topics: log.topics.map(hash), data: log.data};
  if (!/^0x(?:[0-9a-fA-F]{2})*$/.test(result.data)) throw Error('Invalid log data');
  if (log.transactionIndex !== undefined && log.transactionIndex !== null) result.transactionIndex = integer(log.transactionIndex);
  result.blockTimestamp = log.blockTimestamp == null ? null : new Date(log.blockTimestamp).toISOString();
  return result;
}
export const eventKey = e => `${e.transactionHash}:${e.logIndex}`;
export const order = (a,b) => a.blockNumber-b.blockNumber || a.logIndex-b.logIndex;
export function decode(log, m) {
  if (log.address === addr(m.marketplace)) {
    const parsed = marketEvents.parseLog(log);
    if (!parsed) return null; // Admin/approval events are journalled but have no projection.
    const args = {};
    parsed.fragment.inputs.forEach((input,i) => { args[input.name] = input.baseType === 'tuple' ? Object.fromEntries(input.components.map((c,j) => [c.name, copy(parsed.args[i][j])])) : copy(parsed.args[i]); });
    return {name: parsed.name, args};
  }
  if (log.topics[0] !== id('Transfer(address,address,uint256)')) return null;
  const isNft = log.address === addr(m.nft);
  if (!isNft && log.address !== addr(m.printer)) throw Error('Unauthorized log address');
  const p = (isNft ? nftTransfer : transfer).parseLog(log);
  return {name: isNft ? 'NFTTransfer' : 'PrinterTransfer', args: {from:p.args.from.toLowerCase(), to:p.args.to.toLowerCase(), value:p.args[2].toString()}};
}
function checkRow(row, m) {
  if (addr(row.assetContract) !== addr(m.nft) || addr(row.currency) !== addr(m.printer)) throw Error('Unsupported asset/currency');
  for (const key of ['tokenId','quantity', 'pricePerToken','totalPrice','startTimestamp','endTimestamp','expirationTimestamp']) if (row[key] !== undefined) tokenId(row[key]);
  if (row.quantity !== '1' || row.tokenType !== '0' || row.status !== '1') throw Error('Only active ERC721 single-token records supported');
}
export function project(events, manifest, seed = null) {
  const m = validateManifest(manifest);
  // Seeds are only the explicitly loaded current rows whose entity IDs are
  // touched by this block. Full rebuild callers may omit the seed entirely.
  const state = {listings:copy(seed?.listings ?? {}), offers:copy(seed?.offers ?? {}), sales:{}, transfers:{}};
  const seen = new Map();
  for (const e of [...events].sort(order)) {
    if (![m.marketplace,m.nft,m.printer].map(addr).includes(e.address)) throw Error('Unauthorized log address');
    const key = eventKey(e);
    if (seen.has(key)) { if (seen.get(key) !== json(e)) throw Error('Conflicting duplicate log'); continue; }
    seen.set(key,json(e));
    const d = decode(e,m); if (!d) continue;
    const a = d.args;
    if (['PlatformFeeInfoUpdated','FlatPlatformFeeUpdated','PlatformFeeTypeUpdated','RoyaltyEngineUpdated'].includes(d.name)) continue;
    if (d.name.endsWith('Transfer')) { state.transfers[key] = {...a,kind:d.name,transactionHash:e.transactionHash,logIndex:e.logIndex}; continue; }
    const isOffer = d.name.includes('Offer');
    const records = isOffer ? state.offers : state.listings;
    const recordId = String(isOffer ? a.offerId : a.listingId);
    const prior = records[recordId];
    if (['NewListing','UpdatedListing','NewOffer'].includes(d.name)) {
      const row = copy(a.listing ?? a.offer); checkRow(row,m);
      if (String(row[isOffer ? 'offerId':'listingId']) !== recordId || addr(row.assetContract) !== addr(a.assetContract) || addr(row[isOffer ? 'offeror':'listingCreator']) !== addr(a[isOffer ? 'offeror':'listingCreator'])) throw Error('Event tuple mismatch');
      if (d.name === 'UpdatedListing') {
        if (!prior || prior.status !== '1') throw Error('Missing/closed listing before update');
        if(row.tokenId!==prior.tokenId || addr(row.assetContract)!==addr(prior.assetContract)
          || addr(row.listingCreator)!==addr(prior.listingCreator)) throw Error('Listing identity changed');
      } else if (prior) throw Error('Duplicate entity creation');
      records[recordId] = {...row, event:key, updates:(prior?.updates ?? 0)+(d.name === 'UpdatedListing' ? 1:0)};
      continue;
    }
    if (!prior || prior.status !== '1') throw Error('Missing/closed entity; deployment history or verified seed required');
    const creator = isOffer ? 'offeror':'listingCreator';
    if (addr(a[creator]) !== addr(prior[creator])) throw Error('Creator mismatch');
    if (d.name.startsWith('Cancelled')) { prior.status = '3'; prior.event=key; continue; }
    if (['NewSale','AcceptedOffer'].includes(d.name)) {
      if (addr(a.assetContract) !== addr(m.nft) || a.tokenId !== prior.tokenId || a.quantityBought !== '1') throw Error('Sale asset mismatch');
      const total = tokenId(a.totalPricePaid);
      // Alternate approved currencies/prices are outside this PRINTER-only projection.
      if (total !== tokenId(isOffer ? prior.totalPrice : prior.pricePerToken)) throw Error('Sale price mismatch');
      prior.status='2'; prior.quantity='0'; prior.event=key;
      state.sales[key] = {kind:d.name,recordId,tokenId:a.tokenId,payer:addr(isOffer ? a.offeror:a.buyer),seller:addr(isOffer ? a.seller:a.listingCreator),totalPrice:total.toString(),expectedCurrency:addr(prior.currency),currency:null,transactionHash:e.transactionHash,logIndex:e.logIndex};
    }
  }
  reconcile(state,m);
  return state;
}
function reconcile(state,m) {
  for (const sale of Object.values(state.sales)) {
    const peers = Object.values(state.sales).filter(s => s.transactionHash === sale.transactionHash);
    const transfers = Object.entries(state.transfers).filter(([,t]) => t.transactionHash === sale.transactionHash);
    sale.evidence = transfers.map(([key]) => key);
    sale.reconciliation = 'UNRESOLVED'; sale.reason='PRINTER/NFT mismatch';
    if (peers.length !== 1) { sale.reason='Multiple sales in transaction; attribution requires reviewed trace'; continue; }
    const nft = transfers.filter(([,t]) => t.kind === 'NFTTransfer' && t.value === sale.tokenId && t.from === sale.seller && !/^0x0{40}$/.test(t.to));
    const net = new Map();
    for (const [,t] of transfers.filter(([,t]) => t.kind === 'PrinterTransfer')) {
      net.set(t.from,(net.get(t.from) ?? 0n)-BigInt(t.value)); net.set(t.to,(net.get(t.to) ?? 0n)+BigInt(t.value));
    }
    const roles = new Map((m.feeRecipients ?? []).map(r => [addr(r.address),r.role]));
    if (sale.payer === sale.seller || roles.has(sale.payer) || roles.has(sale.seller)) { sale.reason='Overlapping payout roles'; continue; }
    const total = BigInt(sale.totalPrice);
    const unknown = [...net].some(([account,delta]) => delta !== 0n && account !== sale.payer && account !== sale.seller && !roles.has(account));
    if (nft.length !== 1 || unknown || net.get(sale.payer) !== -total || [...net].some(([account,delta]) => account !== sale.payer && delta < 0n)) continue;
    if (sale.kind === 'AcceptedOffer' && nft[0][1].to !== sale.payer) continue;
    sale.reconciliation='MATCHED'; sale.currency=addr(m.printer); sale.reason=null; sale.recipient=nft[0][1].to;
    sale.sellerProceeds=(net.get(sale.seller) ?? 0n).toString();
    sale.fees=[...roles].map(([account,role]) => ({address:account,role,amount:(net.get(account) ?? 0n).toString()}));
  }
}

export async function verifyChain(client, manifest, blockNumber) {
  const m=validateManifest(manifest);
  if (await client.getChainId() !== 4663) throw Error('RPC is not Robinhood Mainnet 4663');
  for (const key of ['nft','printer','marketplace']) {
    const code=await client.getBytecode({address:addr(m[key]),blockNumber:BigInt(blockNumber)});
    const actualHash = code && code !== '0x' ? keccak256(code) : 'NO_CODE';  if (actualHash !== m.codeHashes[key]) {   process.stdout.write(JSON.stringify({     event: 'bytecode_diagnostic',     contract: key,     blockNumber,     expectedHash: m.codeHashes[key],     actualHash   }) + '
');    throw Error(`Unauthorized ${key} bytecode`); }
  }
  const decimals=await client.readContract({address:addr(m.printer),abi:[{type:'function',name:'decimals',stateMutability:'view',inputs:[],outputs:[{type:'uint8'}]}],functionName:'decimals',blockNumber:BigInt(blockNumber)});
  if (decimals !== m.printerDecimals) throw Error('PRINTER decimals do not match the attested manifest');
}

// Robinhood Mainnet's Alchemy eth_getLogs range is limited to ten inclusive
// blocks. Keep the cap here, at the only reader used by normal catch-up and
// restart paths, so a larger worker batch cannot exceed the provider limit.
export const MAX_ETH_GET_LOGS_BLOCKS = 10;
export async function getLogsInRanges({client,address,fromBlock,toBlock}) {
  const from=typeof fromBlock==='bigint'?fromBlock:BigInt(integer(fromBlock));
  const to=typeof toBlock==='bigint'?toBlock:BigInt(integer(toBlock));
  if(from<0n||to<0n)throw Error('Invalid eth_getLogs range');
  if(from>to)return [];
  const unique=new Map();
  for(let first=from;first<=to;) {
    const last=to<first+BigInt(MAX_ETH_GET_LOGS_BLOCKS-1)?to:first+BigInt(MAX_ETH_GET_LOGS_BLOCKS-1);
    const page=await client.getLogs({address,fromBlock:first,toBlock:last});
    if(!Array.isArray(page))throw Error('Invalid eth_getLogs response');
    for(const log of page) {
      const normalized=normalizeLog(log),key=eventKey(normalized),serialized=json(normalized);
      const prior=unique.get(key);
      if(prior&&prior.serialized!==serialized)throw Error('Conflicting duplicate eth_getLogs log');
      if(!prior)unique.set(key,{log,normalized,serialized});
    }
    first=last+1n;
  }
  return [...unique.values()]
    .sort((a,b)=>order(a.normalized,b.normalized))
    .map(item=>item.log);
}

// Reader is a viem public client backed by same-origin /rpc. This module has no
// provider, credentials, environment lookup, signing, SQL connection or scheduler.
export async function readBatch({client,manifest,checkpoint,toBlock,confirmations=12,head:observedHead=null,anchor:observedAnchor=null}) {
  validateManifest(manifest); integer(confirmations); if (confirmations < 1) throw Error('Confirmations required');
  integer(toBlock); const start=integer(Number(checkpoint.block_number));
  if (toBlock <= start || toBlock-start > 1000) throw Error('Batch must contain 1–1000 blocks');
  const head=observedHead??await client.getBlockNumber();
  if (BigInt(toBlock)+BigInt(confirmations) > head) throw Error('Unconfirmed batch');
  const anchor=observedAnchor??await client.getBlock({blockNumber:BigInt(start)});
  if (anchor.hash !== checkpoint.block_hash) throw Error('Reorg: recovery required');
  const blocks=[]; const timestamps=new Map();let parent=anchor.hash;
  for (let n=start+1;n<=toBlock;n++) {
    const b=await client.getBlock({blockNumber:BigInt(n)});
    if(b.parentHash!==parent)throw Error('Reorg during collection');
    blocks.push({number:n,hash:b.hash,parentHash:b.parentHash});parent=b.hash;
    if (b.timestamp !== undefined) timestamps.set(n,new Date(Number(b.timestamp)*1000).toISOString());
  }
  const logs=await getLogsInRanges({client,address:[manifest.marketplace,manifest.printer,manifest.nft].map(addr),fromBlock:start+1,toBlock});
  const txs=new Set(logs.map(l=>l.transactionHash)); const receipts=[];
  for (const tx of txs) {
    const r=await client.getTransactionReceipt({hash:tx});
    if (r.status !== 'success' || r.transactionHash !== tx) throw Error('Missing/failed receipt');
    const relevant=r.logs.filter(l=>[manifest.marketplace,manifest.printer,manifest.nft].map(addr).includes(addr(l.address))).map(l=>normalizeLog({...l,transactionIndex:l.transactionIndex ?? r.transactionIndex,blockTimestamp:timestamps.get(Number(l.blockNumber)) ?? null}));
    if (relevant.some(l=>l.transactionHash !== tx || l.blockHash !== r.blockHash || l.blockNumber !== Number(r.blockNumber))) throw Error('Receipt identity mismatch');
    receipts.push(...relevant);
  }
  const receiptMap=new Map(receipts.map(e=>[eventKey(e),json(e)]));
  if (logs.some(l=>{ const e=normalizeLog({...l,blockTimestamp:timestamps.get(Number(l.blockNumber)) ?? null}); return receiptMap.get(eventKey(e)) !== json(e); })) throw Error('Log/receipt mismatch');
  // A canonical end-block hash commits to every parent. The collected headers
  // were checked as a continuous chain above, and the writer checks the same
  // parent links again before COMMIT, so rereading every header is redundant.
  const canonicalEnd=await client.getBlock({blockNumber:BigInt(toBlock)});
  if(canonicalEnd.hash!==blocks.at(-1)?.hash)throw Error('Reorg during collection');
  // Verify runtime code and PRINTER decimals once after collection and before
  // any SQL commit. No cross-batch cache is used: proxy/immutability status for
  // the future mainnet Marketplace has not yet been attested.
  await verifyChain(client,manifest,toBlock);
  return {expectedVersion:String(checkpoint.version),blocks,events:receipts};
}
