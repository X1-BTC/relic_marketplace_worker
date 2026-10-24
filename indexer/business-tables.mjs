import {addr, copy, decode, eventKey, integer, json, normalizeLog, order} from './marketplace.mjs';
import {tokenId} from '../shared/safety.mjs';

export const SNAPSHOT_BLOCK = 41798460;
// Literal schema supplied by the owner. Identifiers below never come from input.
export const TABLE_KEYS = Object.freeze({
  relic_market_raw_events:['chain_id','transaction_hash','log_index'],
  relic_market_activity:['chain_id','transaction_hash','log_index','event_ordinal'],
  relic_market_config_events:['chain_id','transaction_hash','log_index'],
  relic_market_listings:['chain_id','marketplace_address','listing_id'],
  relic_market_offers:['chain_id','marketplace_address','offer_id'],
  relic_market_settlements:['chain_id','transaction_hash','sale_log_index'],
  relic_market_payment_transfers:['chain_id','transaction_hash','log_index'],
});
// Whitelist of the exact supplied columns used by this adapter, not user input.
const COLUMNS = {
  relic_market_activity:'chain_id marketplace_address transaction_hash log_index block_number transaction_index block_hash block_timestamp event_ordinal event_kind listing_id offer_id nft_contract token_id actor_address seller_address buyer_address currency_address amount_raw quantity start_time end_time details ingest_source',
  relic_market_config_events:'chain_id marketplace_address transaction_hash log_index block_number transaction_index block_hash block_timestamp event_name recipient_address fee_bps decoded',
  relic_market_listings:'chain_id marketplace_address nft_contract token_id currency_address status last_block_number last_log_index last_transaction_hash last_transaction_index chain_status_code listing_id seller_address price_per_token_raw quantity start_time end_time reserved token_type',
  relic_market_offers:'chain_id marketplace_address nft_contract token_id currency_address status last_block_number last_log_index last_transaction_hash last_transaction_index chain_status_code offer_id offeror_address total_price_raw expiration_time',
  relic_market_settlements:'chain_id marketplace_address transaction_hash block_number transaction_index block_hash block_timestamp sale_log_index settlement_type listing_id offer_id nft_contract token_id buyer_address seller_address currency_address currency_decimals quantity_raw gross_amount_raw seller_proceeds_raw platform_fee_raw thirdweb_fee_raw royalty_fee_raw marketplace_residual_raw reconcile_status reconciliation',
  relic_market_payment_transfers:'chain_id transaction_hash log_index block_number transaction_index block_hash token_contract from_address to_address amount_raw sale_log_index payment_role',
};
const kinds = Object.freeze({NewListing:'LISTING_CREATED',UpdatedListing:'LISTING_UPDATED',CancelledListing:'LISTING_CANCELLED',NewSale:'LISTING_SOLD',NewOffer:'OFFER_CREATED',CancelledOffer:'OFFER_CANCELLED',AcceptedOffer:'OFFER_ACCEPTED',PrinterTransfer:'PRINTER_TRANSFER'});
export const CONFIG_EVENTS = new Set(['PlatformFeeInfoUpdated','FlatPlatformFeeUpdated','PlatformFeeTypeUpdated','RoyaltyEngineUpdated']);
const toTimestamp = (value, openEnd=false) => {
  if (value == null) return null;
  const n=tokenId(value);
  if (openEnd && [((1n<<128n)-1n),((1n<<256n)-1n)].includes(n)) return null;
  if (n > 253402300799n) throw Error('Timestamp cannot be represented exactly by business timestamp mapping');
  return new Date(Number(n)*1000).toISOString();
};
export function rawPayload(row) {
  return normalizeLog({address:row.contract_address,transactionHash:row.transaction_hash,logIndex:row.log_index,blockNumber:Number(row.block_number),blockHash:row.block_hash,transactionIndex:row.transaction_index,blockTimestamp:row.block_timestamp,topics:row.topics,data:row.data,removed:row.removed});
}
export function rawRow(e, manifest) {
  integer(e.transactionIndex);
  if (e.blockNumber <= SNAPSHOT_BLOCK) throw Error('Protected snapshot/BACKFILL range');
  const d=decode(e,manifest);
  return {chain_id:4663,contract_address:e.address,transaction_hash:e.transactionHash,log_index:e.logIndex,block_number:e.blockNumber,block_hash:e.blockHash,block_timestamp:e.blockTimestamp,topic0:e.topics[0] ?? null,topics:json(e.topics),data:e.data,event_name:d?.name ?? null,decoded:d ? json(d.args):null,ingest_source:'LIVE',transaction_index:e.transactionIndex,removed:false};
}
function provenance(e,m) {
  integer(e.transactionIndex);
  if (e.blockNumber <= SNAPSHOT_BLOCK) throw Error('Protected snapshot/BACKFILL range');
  return {chain_id:4663,marketplace_address:addr(m.marketplace),transaction_hash:e.transactionHash,log_index:e.logIndex,block_number:e.blockNumber,transaction_index:e.transactionIndex,block_hash:e.blockHash,block_timestamp:e.blockTimestamp};
}
// Only listing/offer transitions change a current entity row. Transfer and
// configuration logs remain event-only and never cause entity reads/writes.
export function entityReferences(events, manifest) {
  const listings=new Set(),offers=new Set();
  for(const e of events){
    const d=decode(e,manifest); if(!d) continue;
    if(['NewListing','UpdatedListing','CancelledListing','NewSale'].includes(d.name))
      listings.add(String(d.args.listingId));
    else if(['NewOffer','CancelledOffer','AcceptedOffer'].includes(d.name))
      offers.add(String(d.args.offerId));
  }
  const numeric=(a,b)=>BigInt(a)<BigInt(b)?-1:BigInt(a)>BigInt(b)?1:0;
  return {listings:[...listings].sort(numeric),offers:[...offers].sort(numeric)};
}
export function businessRows(events,state,m,stream) {
  const rows=Object.fromEntries(Object.keys(TABLE_KEYS).filter(t=>t!=='relic_market_raw_events').map(t=>[t,[]]));
  const byKey=new Map(events.map(e=>[eventKey(e),e]));
  const marketTx=new Set(events.filter(e=>e.address===addr(m.marketplace)).map(e=>e.transactionHash));
  const saleByKey=state.sales;
  for (const e of [...events].sort(order)) {
    const p=provenance(e,m); const d=decode(e,m); const a=d?.args ?? {};
    const entity=d?.name.includes('Offer') ? state.offers[String(a.offerId)] : state.listings[String(a.listingId)];
    const tuple=a.listing ?? a.offer;
    const sale=saleByKey[eventKey(e)];
    if (e.address===addr(m.marketplace) || (d?.name==='PrinterTransfer' && marketTx.has(e.transactionHash))) {
      rows.relic_market_activity.push({...p,event_ordinal:0,event_kind:kinds[d?.name] ?? 'OTHER',listing_id:a.listingId ?? null,offer_id:a.offerId ?? null,nft_contract:tuple?.assetContract ? addr(tuple.assetContract) : entity?.assetContract ? addr(entity.assetContract):null,token_id:tuple?.tokenId ?? a.tokenId ?? entity?.tokenId ?? null,actor_address:sale?.payer ?? (a.listingCreator ? addr(a.listingCreator):a.offeror ? addr(a.offeror):a.from ?? null),seller_address:sale?.seller ?? (entity?.listingCreator ? addr(entity.listingCreator):null),buyer_address:sale?.payer ?? null,currency_address:sale ? sale.currency : tuple?.currency ? addr(tuple.currency):d?.name==='PrinterTransfer' ? addr(m.printer):null,amount_raw:sale?.totalPrice ?? tuple?.pricePerToken ?? tuple?.totalPrice ?? (d?.name==='PrinterTransfer'?a.value:null),quantity:a.quantityBought ?? tuple?.quantity ?? null,start_time:toTimestamp(tuple?.startTimestamp),end_time:toTimestamp(tuple?.endTimestamp ?? tuple?.expirationTimestamp,true),details:json({indexer_stream:stream,event_name:d?.name ?? null,decoded:a,recipient:sale?.recipient ?? null}),ingest_source:'LIVE'});
    }
    if (CONFIG_EVENTS.has(d?.name)) {
      const bps=a.platformFeeBps;
      if (bps !== undefined && tokenId(bps)>10000n) throw Error('Invalid platform fee bps');
      rows.relic_market_config_events.push({...p,event_name:d.name,recipient_address:a.platformFeeRecipient ? addr(a.platformFeeRecipient):a.newAddress ? addr(a.newAddress):null,fee_bps:bps===undefined?null:Number(bps),decoded:json(a)});
    }
  }
  for (const [kind,records] of [['listings',state.listings],['offers',state.offers]]) for (const [id,r] of Object.entries(records)) {
    const e=byKey.get(r.event); if (!e) throw Error('Missing entity provenance');
    const base={chain_id:4663,marketplace_address:addr(m.marketplace),nft_contract:addr(r.assetContract),token_id:r.tokenId,currency_address:addr(r.currency),status:r.status==='1'?'ACTIVE':r.status==='3'?'CANCELLED':kind==='listings'?'SOLD':'ACCEPTED',last_block_number:e.blockNumber,last_log_index:e.logIndex,last_transaction_hash:e.transactionHash,last_transaction_index:e.transactionIndex,chain_status_code:Number(r.status)};
    rows[`relic_market_${kind}`].push(kind==='listings'?{...base,listing_id:id,seller_address:addr(r.listingCreator),price_per_token_raw:r.pricePerToken,quantity:r.quantity,start_time:toTimestamp(r.startTimestamp),end_time:toTimestamp(r.endTimestamp,true),reserved:r.reserved,token_type:Number(r.tokenType)}:{...base,offer_id:id,offeror_address:addr(r.offeror),total_price_raw:r.totalPrice,expiration_time:toTimestamp(r.expirationTimestamp,true)});
  }
  for (const [key,s] of Object.entries(state.sales)) {
    const e=byKey.get(key); const {log_index,...p}=provenance(e,m); const matched=s.reconciliation==='MATCHED';
    const amount=role=>matched?s.fees.filter(f=>f.role===role).reduce((n,f)=>n+BigInt(f.amount),0n).toString():null;
    rows.relic_market_settlements.push({...p,sale_log_index:log_index,settlement_type:s.kind==='NewSale'?'LISTING_SOLD':'OFFER_ACCEPTED',listing_id:s.kind==='NewSale'?s.recordId:null,offer_id:s.kind==='AcceptedOffer'?s.recordId:null,nft_contract:addr(m.nft),token_id:s.tokenId,buyer_address:s.payer,seller_address:s.seller,currency_address:s.currency ?? s.expectedCurrency,currency_decimals:matched?m.printerDecimals:null,quantity_raw:'1',gross_amount_raw:s.totalPrice,seller_proceeds_raw:matched?s.sellerProceeds:null,platform_fee_raw:amount('platform'),thirdweb_fee_raw:amount('thirdweb'),royalty_fee_raw:amount('royalty'),marketplace_residual_raw:matched?'0':null,reconcile_status:matched?'RECONCILED':'AMBIGUOUS',reconciliation:json(s)});
  }
  for (const [key,t] of Object.entries(state.transfers)) {
    if (t.kind!=='PrinterTransfer') continue;
    const e=byKey.get(key); const {marketplace_address,block_timestamp,...p}=provenance(e,m);
    const sales=Object.values(state.sales).filter(s=>s.transactionHash===t.transactionHash && s.reconciliation==='MATCHED');
    const s=sales.length===1?sales[0]:null;
    // Role is the observed destination's role; intermediate transfers remain
    // UNATTRIBUTED even when their transaction has a reconciled settlement.
    let role='UNATTRIBUTED';
    if (s && t.to===s.seller) role='SELLER';
    else if (s) { const f=s.fees.find(f=>f.address===t.to); if(f) role=f.role.toUpperCase(); }
    rows.relic_market_payment_transfers.push({...p,token_contract:addr(m.printer),from_address:t.from,to_address:t.to,amount_raw:t.value,sale_log_index:s?.logIndex ?? null,payment_role:role});
  }
  return copy(rows);
}

// Only locally constructed row keys / literal known tables reach SQL identifiers.
export async function upsertBusinessRow(db,table,row,stream) {
  const keys=TABLE_KEYS[table]; if (!keys || table==='relic_market_raw_events') throw Error('Unknown writable business table');
  const columns=Object.keys(row);
  if(columns.some(c=>!COLUMNS[table].split(' ').includes(c)) || keys.some(k=>!columns.includes(k))) throw Error('Unexpected business column');
  const values=columns.map(c=>row[c]);
  const entity=['relic_market_listings','relic_market_offers'].includes(table);
  const tx=entity?'last_transaction_hash':'transaction_hash';
  const log=entity?'last_log_index':table==='relic_market_settlements'?'sale_log_index':'log_index';
  const source=table==='relic_market_activity'?` AND public.${table}.ingest_source='LIVE'`:'';
  const updates=columns.filter(c=>!keys.includes(c)).map(c=>`${c}=EXCLUDED.${c}`);
  if(entity) updates.push('updated_at=now()');
  const sql=`INSERT INTO public.${table}(${columns.join(',')}) VALUES(${values.map((_,i)=>'$'+(i+1)).join(',')}) ON CONFLICT(${keys.join(',')}) DO UPDATE SET ${updates.join(',')} WHERE EXISTS (SELECT 1 FROM relic_market_indexer.events j WHERE j.stream=$${values.length+1} AND j.transaction_hash=public.${table}.${tx} AND j.log_index=public.${table}.${log})${source} RETURNING ${keys[0]}`;
  const result=await db.query(sql,[...values,stream]);
  if(result.rows.length!==1) throw Error('Refusing to overwrite a row outside this journal or a BACKFILL row');
}
