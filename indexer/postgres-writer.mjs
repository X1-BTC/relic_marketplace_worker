import {copy,json,validateManifest,manifestDigest,normalizeLog,eventKey,project,hash,integer,verifyChain} from './marketplace.mjs';
import {SNAPSHOT_BLOCK, rawPayload, rawRow, businessRows, entityReferences, upsertBusinessRow} from './business-tables.mjs';

// pg-compatible Pool dependency injection. No default URL or environment access.
// Only an explicitly supplied pool can write; application /api/data stays read-only.
export class PostgresMarketplaceWriter {
  constructor({pool,stream,manifest}) {
    this.pool=pool;
    if (typeof stream !== 'string' || !/^[a-z0-9_-]{1,80}$/.test(stream)) throw Error('Invalid stream');
    this.stream=stream; this.manifest=Object.freeze(validateManifest(manifest));
    this.digest=manifestDigest(this.manifest);
  }
  check(cp) {
    if (manifestDigest(this.manifest) !== this.digest) throw Error('Manifest changed');
    if (!cp || cp.chain_id !== 4663 || cp.manifest_digest !== this.digest || manifestDigest(cp.manifest) !== this.digest) throw Error('Missing checkpoint or manifest mismatch');
    hash(cp.block_hash); hash(cp.anchor_hash);
    integer(Number(cp.block_number)); integer(Number(cp.anchor_number));
    if (!/^(0|[1-9]\d*)$/.test(String(cp.version))) throw Error('Invalid checkpoint version');
    return cp;
  }
  async transaction(work) {
    let db; let begun=false;
    try {
      db=await this.pool.connect();
      await db.query('BEGIN'); begun=true;
      const result=await work(db);
      await db.query('COMMIT'); return result;
    } catch (error) {
      if (begun) { try { await db.query('ROLLBACK'); } catch { /* release destroys unusable connection below */ } }
      // Never include connection strings, SQL parameter values, or driver errors.
      const safeCode=/^[0-9A-Z]{5}$/.test(String(error.code??''))?`Database SQLSTATE ${error.code}`:'Database or projection failure';
      throw new Error('Indexer transaction failed; reread checkpoint before retry', {cause: new Error(error.indexerReason ?? safeCode)});
    } finally { db?.release(true); } // discard also after uncertain COMMIT / rollback failure
  }
  async checkpoint() {
    let db;
    try { db=await this.pool.connect(); const {rows}=await db.query('SELECT * FROM relic_market_indexer.checkpoints WHERE stream=$1',[this.stream]); return copy(this.check(rows[0])); }
    catch (error) {   const reason =     error?.message === 'Missing checkpoint or manifest mismatch'       ? 'CHECKPOINT_MANIFEST_MISMATCH'       : error?.message === 'Invalid checkpoint version'         ? 'INVALID_CHECKPOINT_VERSION'         : error?.code && /^[0-9A-Z]{5}$/.test(error.code)           ? `POSTGRES_${error.code}`           : 'CHECKPOINT_READ_OR_VALIDATION_FAILED';    throw new Error(     `Unable to read verified indexer checkpoint: ${reason}`   ); }
    finally { db?.release(); }
  }
  async advanceCheckpoint({ blockNumber, blockHash, expectedVersion }) {
    return this.transaction(async db => {
      const cp = await this.locked(db, expectedVersion);
      if (Number(cp.block_number) >= blockNumber) return { unchanged: true };
      const nextVersion = (BigInt(cp.version) + 1n).toString();
      const advanced = await db.query(
        `UPDATE relic_market_indexer.checkpoints 
         SET block_number = $2, block_hash = $3, version = version + 1 
         WHERE stream = $1 AND version = $4 
         RETURNING version`,
        [this.stream, blockNumber, blockHash, cp.version]
      );
      if (advanced.rows.length !== 1) throw Error('Checkpoint advance compare-and-swap failed');
      return { blockNumber, blockHash, version: nextVersion };
    });
  }
  async locked(db,version) {
    const {rows}=await db.query('SELECT * FROM relic_market_indexer.checkpoints WHERE stream=$1 FOR UPDATE',[this.stream]);
    const cp=this.check(rows[0]);
    if (String(cp.version) !== String(version)) throw Error('Stale checkpoint');
    return cp;
  }
  async registry(db, firstBlock) {
    const {rows}=await db.query('SELECT role,contract_address,start_block FROM public.relic_market_contracts WHERE chain_id=4663 AND enabled=true AND contract_address=ANY($1::text[])',[[this.manifest.marketplace,this.manifest.printer,this.manifest.nft].map(v=>v.toLowerCase())]);
    const expected={MARKETPLACE_V3:this.manifest.marketplace,PRINTER:this.manifest.printer,RELIC_NFT:this.manifest.nft};
    if(rows.length!==3) throw Error('Three enabled registry contracts required');
    for(const [role,contract] of Object.entries(expected)) {
      const entry=rows.find(r=>r.role===role && r.contract_address===contract.toLowerCase());
      if(!entry || BigInt(entry.start_block)>BigInt(firstBlock)) throw Error('Registry manifest/start block mismatch');
    }
  }
  async journal(db, afterBlock=null) {
    // Used only for explicit reconstruction or the exact orphan range during a
    // reorg. The normal commit path never calls this full-journal reader.
    const {rows}=await db.query('SELECT r.*,j.block_number AS journal_block_number,b.block_hash AS journal_block_hash FROM relic_market_indexer.events j JOIN relic_market_indexer.blocks b ON b.stream=j.stream AND b.block_number=j.block_number JOIN public.relic_market_raw_events r ON r.chain_id=j.chain_id AND r.transaction_hash=j.transaction_hash AND r.log_index=j.log_index WHERE j.stream=$1 AND ($2::bigint IS NULL OR j.block_number>$2) ORDER BY j.block_number,j.log_index',[this.stream,afterBlock]);
    if(rows.some(r=>r.ingest_source!=='LIVE' || r.removed || String(r.block_number)!==String(r.journal_block_number) || r.block_hash!==r.journal_block_hash)) throw Error('Protected or removed journal event');
    return rows.map(rawPayload);
  }
  async rebuildFromJournal(db, { confirmation } = {}) {
    // Deliberate maintenance operation only. It is never called by commit()
    // or recover(); normal projection and rollback are incremental.
    if (confirmation !== 'REBUILD_MARKETPLACE_PROJECTIONS') {
      throw Error('Explicit Marketplace projection rebuild confirmation required');
    }
    const events=await this.journal(db);
    const state=project(events,this.manifest);
    const rows=businessRows(events,state,this.manifest,this.stream);
    // Deferrable payment FK is also respected by writing settlements first.
    for(const table of ['relic_market_activity','relic_market_config_events','relic_market_listings','relic_market_offers','relic_market_settlements','relic_market_payment_transfers']) {
      for(const row of rows[table]) await upsertBusinessRow(db,table,row,this.stream);
    }
    return state;
  }
  async readEntitySeeds(db,refs) {
    const state={listings:{},offers:{},sales:{},transfers:{}};
    if(refs.listings.length){
      const {rows}=await db.query(`SELECT listing_id::text,token_id::text,quantity::text,price_per_token_raw::text,
        coalesce(trunc(extract(epoch FROM start_time))::text,'0') AS start_timestamp,
        coalesce(trunc(extract(epoch FROM end_time))::text,'340282366920938463463374607431768211455') AS end_timestamp,
        lower(seller_address) AS listing_creator,lower(nft_contract) AS asset_contract,lower(currency_address) AS currency,
        coalesce(token_type,0)::text AS token_type,coalesce(chain_status_code,CASE status WHEN 'ACTIVE' THEN 1 WHEN 'SOLD' THEN 2 ELSE 3 END)::text AS chain_status_code,
        reserved,last_transaction_hash,last_log_index
        FROM public.relic_market_listings WHERE chain_id=4663 AND marketplace_address=$1 AND listing_id=ANY($2::numeric[])`,[this.manifest.marketplace.toLowerCase(),refs.listings]);
      for(const r of rows)state.listings[r.listing_id]={listingId:r.listing_id,tokenId:r.token_id,quantity:r.quantity,pricePerToken:r.price_per_token_raw,startTimestamp:r.start_timestamp,endTimestamp:r.end_timestamp,listingCreator:r.listing_creator,assetContract:r.asset_contract,currency:r.currency,tokenType:r.token_type,status:r.chain_status_code,reserved:r.reserved,event:`${r.last_transaction_hash}:${r.last_log_index}`,updates:0};
    }
    if(refs.offers.length){
      const {rows}=await db.query(`SELECT offer_id::text,token_id::text,'1'::text AS quantity,total_price_raw::text,
        coalesce(trunc(extract(epoch FROM expiration_time))::text,'115792089237316195423570985008687907853269984665640564039457584007913129639935') AS expiration_timestamp,
        lower(offeror_address) AS offeror,lower(nft_contract) AS asset_contract,lower(currency_address) AS currency,
        '0'::text AS token_type,coalesce(chain_status_code,CASE status WHEN 'ACTIVE' THEN 1 WHEN 'ACCEPTED' THEN 2 ELSE 3 END)::text AS chain_status_code,
        last_transaction_hash,last_log_index FROM public.relic_market_offers
        WHERE chain_id=4663 AND marketplace_address=$1 AND offer_id=ANY($2::numeric[])`,[this.manifest.marketplace.toLowerCase(),refs.offers]);
      for(const r of rows)state.offers[r.offer_id]={offerId:r.offer_id,tokenId:r.token_id,quantity:r.quantity,totalPrice:r.total_price_raw,expirationTimestamp:r.expiration_timestamp,offeror:r.offeror,assetContract:r.asset_contract,currency:r.currency,tokenType:r.token_type,status:r.chain_status_code,event:`${r.last_transaction_hash}:${r.last_log_index}`};
    }
    return state;
  }
  async captureEntityUndo(db,blockNumber,refs) {
    const state={listings:{},offers:{},sales:{},transfers:{}};
    for(const kind of ['listings','offers'])if(refs[kind].length){
      const table=kind==='listings'?'relic_market_listings':'relic_market_offers';
      const idColumn=kind==='listings'?'listing_id':'offer_id';
      const {rows}=kind==='listings'
        ? await db.query(`SELECT listing_id::text,token_id::text,quantity::text,price_per_token_raw::text,
          coalesce(trunc(extract(epoch FROM start_time))::text,'0') AS start_timestamp,
          coalesce(trunc(extract(epoch FROM end_time))::text,'340282366920938463463374607431768211455') AS end_timestamp,
          lower(seller_address) AS listing_creator,lower(nft_contract) AS asset_contract,lower(currency_address) AS currency,
          coalesce(token_type,0)::text AS token_type,coalesce(chain_status_code,CASE status WHEN 'ACTIVE' THEN 1 WHEN 'SOLD' THEN 2 ELSE 3 END)::text AS chain_status_code,
          reserved,last_transaction_hash,last_log_index,to_jsonb(r)::text AS before_image
          FROM public.relic_market_listings r WHERE chain_id=4663 AND marketplace_address=$1 AND listing_id=ANY($2::numeric[])`,[this.manifest.marketplace.toLowerCase(),refs[kind]])
        : await db.query(`SELECT offer_id::text,token_id::text,'1'::text AS quantity,total_price_raw::text,
          coalesce(trunc(extract(epoch FROM expiration_time))::text,'115792089237316195423570985008687907853269984665640564039457584007913129639935') AS expiration_timestamp,
          lower(offeror_address) AS offeror,lower(nft_contract) AS asset_contract,lower(currency_address) AS currency,
          '0'::text AS token_type,coalesce(chain_status_code,CASE status WHEN 'ACTIVE' THEN 1 WHEN 'ACCEPTED' THEN 2 ELSE 3 END)::text AS chain_status_code,
          last_transaction_hash,last_log_index,to_jsonb(r)::text AS before_image FROM public.relic_market_offers r
          WHERE chain_id=4663 AND marketplace_address=$1 AND offer_id=ANY($2::numeric[])`,[this.manifest.marketplace.toLowerCase(),refs[kind]]);
      const byId=new Map(rows.map(r=>[r[idColumn],r]));
      for(const id of refs[kind]){
        const r=byId.get(id);
        if(r)state[kind][id]=kind==='listings'
          ? {listingId:r.listing_id,tokenId:r.token_id,quantity:r.quantity,pricePerToken:r.price_per_token_raw,startTimestamp:r.start_timestamp,endTimestamp:r.end_timestamp,listingCreator:r.listing_creator,assetContract:r.asset_contract,currency:r.currency,tokenType:r.token_type,status:r.chain_status_code,reserved:r.reserved,event:`${r.last_transaction_hash}:${r.last_log_index}`,updates:0}
          : {offerId:r.offer_id,tokenId:r.token_id,quantity:r.quantity,totalPrice:r.total_price_raw,expirationTimestamp:r.expiration_timestamp,offeror:r.offeror,assetContract:r.asset_contract,currency:r.currency,tokenType:r.token_type,status:r.chain_status_code,event:`${r.last_transaction_hash}:${r.last_log_index}`};
      }
      const kindName=kind==='listings'?'LISTING':'OFFER';
      const values=[],tuples=[];
      for(const id of refs[kind]){
        const r=byId.get(id);const offset=values.length;
        values.push(this.stream,blockNumber,kindName,id,r?.before_image ?? null);
        tuples.push(`($${offset+1},$${offset+2},$${offset+3},$${offset+4}::numeric,$${offset+5}::jsonb)`);
      }
      await db.query(`INSERT INTO relic_market_indexer.entity_undo(stream,block_number,entity_kind,entity_id,previous_row) VALUES ${tuples.join(',')}`,values);
    }
    return state;
  }
  async restoreEntityUndo(db,ancestorBlock) {
    const {rows}=await db.query(`SELECT block_number,entity_kind,entity_id::text,previous_row::text
      FROM relic_market_indexer.entity_undo WHERE stream=$1 AND block_number>$2
      ORDER BY block_number DESC,entity_kind,entity_id DESC`,[this.stream,ancestorBlock]);
    for(const row of rows){
      const listing=row.entity_kind==='LISTING';
      if(!listing && row.entity_kind!=='OFFER')throw Error('Invalid entity undo kind');
      const table=listing?'relic_market_listings':'relic_market_offers';
      const idColumn=listing?'listing_id':'offer_id';
      if(row.previous_row===null){
        await db.query(`DELETE FROM public.${table} WHERE chain_id=4663 AND marketplace_address=$1 AND ${idColumn}=$2::numeric`,[this.manifest.marketplace.toLowerCase(),row.entity_id]);
        continue;
      }
      const provenance=await db.query(`SELECT EXISTS(SELECT 1 FROM relic_market_indexer.events j
        JOIN public.relic_market_raw_events r ON r.chain_id=j.chain_id AND r.transaction_hash=j.transaction_hash AND r.log_index=j.log_index
        WHERE j.stream=$2 AND r.ingest_source='LIVE' AND NOT r.removed AND r.chain_id=4663
          AND r.transaction_hash=($1::jsonb->>'last_transaction_hash') AND r.log_index=($1::jsonb->>'last_log_index')::integer
          AND r.block_number=($1::jsonb->>'last_block_number')::bigint) AS owned`,[row.previous_row,this.stream]);
      if(!provenance.rows[0]?.owned)throw Error('Undo image lacks surviving LIVE provenance');
      const keys=listing?['chain_id','marketplace_address','listing_id']:['chain_id','marketplace_address','offer_id'];
      const all=listing?['chain_id','marketplace_address','listing_id','nft_contract','token_id','seller_address','currency_address','price_per_token_raw','quantity','start_time','end_time','status','last_block_number','last_log_index','last_transaction_hash','updated_at','last_transaction_index','reserved','token_type','chain_status_code']:
        ['chain_id','marketplace_address','offer_id','nft_contract','token_id','offeror_address','currency_address','total_price_raw','expiration_time','status','last_block_number','last_log_index','last_transaction_hash','updated_at','last_transaction_index','chain_status_code'];
      const updates=all.filter(c=>!keys.includes(c)).map(c=>`${c}=EXCLUDED.${c}`).join(',');
      const restored=await db.query(`WITH candidate AS (SELECT (jsonb_populate_record(NULL::public.${table},$1::jsonb)).*)
        INSERT INTO public.${table}(${all.join(',')}) SELECT ${all.join(',')} FROM candidate
        ON CONFLICT(${keys.join(',')}) DO UPDATE SET ${updates} RETURNING ${idColumn}`,[row.previous_row]);
      if(restored.rows.length!==1)throw Error('Failed to restore Marketplace entity before-image');
    }
    return rows;
  }
  async setVolumeCheckpoint(db,version,blockNumber,blockHash) {
    const {rows}=await db.query(`INSERT INTO relic_market_indexer.wallet_volume_state
      (stream,chain_id,marketplace_address,currency_address,currency_decimals,checkpoint_version,checkpoint_block_number,checkpoint_block_hash)
      VALUES($1,4663,$2,$3,$4,$5,$6,$7)
      ON CONFLICT(stream) DO UPDATE SET checkpoint_version=EXCLUDED.checkpoint_version,
        checkpoint_block_number=EXCLUDED.checkpoint_block_number,checkpoint_block_hash=EXCLUDED.checkpoint_block_hash,updated_at=now()
      WHERE relic_market_indexer.wallet_volume_state.chain_id=466
