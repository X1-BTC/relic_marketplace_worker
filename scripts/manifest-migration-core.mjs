import {json,manifestDigest,validateManifest} from '../indexer/marketplace.mjs';

export const MARKETPLACE='0xb87bbab928495bec8085eca758660790a0c67f9f';
export const NFT='0xb509e195bcb3e4461e235ff152c68d66915f67b5';
export const PRINTER='0x85a574f2ff0795685f58d1d7b0d4b51f148ac489';
export const PLATFORM_FEE='0x87cee0881b121f3557e4b319d35d75dedbfebe2b';
export const THIRDWEB_FEE='0x1af20c6b23373350ad464700b5965ce4b0d2ad94';
export const MARKETPLACE_CODE_HASH='0x3f319ab8d6c857a9015c7158993abfce3610383e5dbdc37fdf8c24a30065f6af';
export const STREAM='relic-mainnet';

const feeKey=rows=>rows.map(x=>`${x.address.toLowerCase()}:${x.role}`).sort();
export function targetManifest(current){
  const source=validateManifest(current);
  if(source.chainId!==4663||source.marketplace.toLowerCase()!==MARKETPLACE||source.nft.toLowerCase()!==NFT||source.printer.toLowerCase()!==PRINTER||source.printerDecimals!==18)throw Error('MANIFEST_CONTRACT_IDENTITY_MISMATCH');
  if(source.codeHashes?.marketplace?.toLowerCase()!==MARKETPLACE_CODE_HASH)throw Error('MANIFEST_MARKETPLACE_CODE_HASH_MISMATCH');
  const oldFees=[{address:PLATFORM_FEE,role:'platform'}],newFees=[...oldFees,{address:THIRDWEB_FEE,role:'thirdweb'}];
  const currentKeys=feeKey(source.feeRecipients??[]);
  if(JSON.stringify(currentKeys)!==JSON.stringify(feeKey(oldFees))&&JSON.stringify(currentKeys)!==JSON.stringify(feeKey(newFees)))throw Error('MANIFEST_FEE_RECIPIENT_CONFLICT');
  const target=validateManifest({...source,feeRecipients:newFees});
  return {current:source,target,currentDigest:manifestDigest(source),targetDigest:manifestDigest(target),alreadyTarget:manifestDigest(source)===manifestDigest(target)};
}
export function priorManifest(current){
  const source=validateManifest(current),currentKeys=feeKey(source.feeRecipients??[]);
  const expected=[{address:PLATFORM_FEE,role:'platform'},{address:THIRDWEB_FEE,role:'thirdweb'}];
  if(JSON.stringify(currentKeys)!==JSON.stringify(feeKey(expected)))throw Error('ROLLBACK_SOURCE_MANIFEST_MISMATCH');
  const target=validateManifest({...source,feeRecipients:[{address:PLATFORM_FEE,role:'platform'}]});
  return {current:source,target,currentDigest:manifestDigest(source),targetDigest:manifestDigest(target),alreadyTarget:manifestDigest(source)===manifestDigest(target)};
}

export function checkpointSnapshot(row){
  if(!row||row.stream!==STREAM||Number(row.chain_id)!==4663)throw Error('CHECKPOINT_IDENTITY_MISMATCH');
  return {stream:row.stream,chain_id:Number(row.chain_id),anchor_number:String(row.anchor_number),anchor_hash:String(row.anchor_hash).toLowerCase(),block_number:String(row.block_number),block_hash:String(row.block_hash).toLowerCase(),version:String(row.version),manifest_digest:String(row.manifest_digest).toLowerCase(),manifest:validateManifest(row.manifest)};
}
export function sameCheckpoint(a,b){
  const x=checkpointSnapshot(a),y=checkpointSnapshot(b);
  return x.stream===y.stream&&x.chain_id===y.chain_id&&x.anchor_number===y.anchor_number&&x.anchor_hash===y.anchor_hash&&x.block_number===y.block_number&&x.block_hash===y.block_hash&&x.version===y.version&&x.manifest_digest===y.manifest_digest&&json(x.manifest)===json(y.manifest);
}
export function assertMaintenanceClear({lockHeld,workerSessions}){
  if(lockHeld||String(workerSessions)!=='0')throw Error('WORKER_OR_WRITER_STILL_ACTIVE');
  return true;
}

// Caller holds the worker advisory lock. This function owns only its SQL
// transaction and changes the two manifest columns; checkpoint position and
// version are compare-only predicates.
export async function applyManifestMigration(db,{expected,target,digest,direction='forward'}){
  let begun=false;
  try{
    await db.query('BEGIN');begun=true;
    const locked=await db.query('SELECT stream,chain_id,anchor_number,anchor_hash,block_number,block_hash,version,manifest_digest,manifest FROM relic_market_indexer.checkpoints WHERE stream=$1 FOR UPDATE',[STREAM]);
    if(locked.rows.length!==1||!sameCheckpoint(locked.rows[0],expected))throw Error('CHECKPOINT_CHANGED_SINCE_PREFLIGHT');
    const current=checkpointSnapshot(locked.rows[0]);
    if(current.manifest_digest!==manifestDigest(current.manifest))throw Error('CHECKPOINT_MANIFEST_DIGEST_INVALID');
    if(current.manifest_digest===digest&&json(current.manifest)===json(target)){
      await db.query('COMMIT');begun=false;return {changed:false,checkpoint:current};
    }
    if(current.manifest_digest!==manifestDigest(current.manifest))throw Error('CHECKPOINT_MANIFEST_DIGEST_INVALID');
    const transition=direction==='rollback'?priorManifest(current.manifest):targetManifest(current.manifest);
    if(transition.targetDigest!==digest||json(transition.target)!==json(target))throw Error('TARGET_MANIFEST_CHANGED');
    const updated=await db.query(`UPDATE relic_market_indexer.checkpoints SET manifest=$2::jsonb,manifest_digest=$3
      WHERE stream=$1 AND chain_id=$4 AND block_number=$5 AND block_hash=$6 AND version=$7 AND manifest_digest=$8
      RETURNING stream,chain_id,anchor_number,anchor_hash,block_number,block_hash,version,manifest_digest,manifest`,
      [STREAM,json(target),digest,current.chain_id,current.block_number,current.block_hash,current.version,current.manifest_digest]);
    if(updated.rows.length!==1)throw Error('CHECKPOINT_COMPARE_AND_SWAP_FAILED');
    const after=checkpointSnapshot(updated.rows[0]);
    if(after.block_number!==current.block_number||after.block_hash!==current.block_hash||after.version!==current.version||after.manifest_digest!==digest||json(after.manifest)!==json(target))throw Error('CHECKPOINT_POSTCONDITION_FAILED');
    await db.query('COMMIT');begun=false;return {changed:true,before:current,checkpoint:after};
  }catch(error){if(begun)try{await db.query('ROLLBACK');}catch{}throw error;}
}
