import assert from 'node:assert/strict';
import {keccak256} from 'ethers';
import {manifestDigest} from '../indexer/marketplace.mjs';
import {targetManifest,priorManifest,checkpointSnapshot,applyManifestMigration,assertMaintenanceClear} from './manifest-migration-core.mjs';

const initial={chainId:4663,nft:'0xb509e195bcb3e4461e235ff152c68d66915f67b5',printer:'0x85a574f2ff0795685f58d1d7b0d4b51f148ac489',marketplace:'0xb87bbab928495bec8085eca758660790a0c67f9f',printerDecimals:18,
  codeHashes:{nft:keccak256('0x11'),printer:keccak256('0x22'),marketplace:'0x3f319ab8d6c857a9015c7158993abfce3610383e5dbdc37fdf8c24a30065f6af'},
  feeRecipients:[{address:'0x87cee0881b121f3557e4b319d35d75dedbfebe2b',role:'platform'}]};
const cp=()=>({stream:'relic-mainnet',chain_id:4663,anchor_number:'99967328',anchor_hash:`0x${'1'.repeat(64)}`,block_number:'110836342',block_hash:`0x${'2'.repeat(64)}`,version:'8880',manifest_digest:manifestDigest(initial),manifest:initial});
const plan=targetManifest(initial);
assert.equal(plan.currentDigest,manifestDigest(initial));
assert.notEqual(plan.targetDigest,plan.currentDigest,'adding Thirdweb changes the project digest');
assert.deepEqual(plan.target.feeRecipients.map(x=>x.role),['platform','thirdweb']);
assert.deepEqual(plan.target.codeHashes,initial.codeHashes,'all code hashes are preserved');
assert.throws(()=>targetManifest({...initial,feeRecipients:[]}),/MANIFEST_FEE_RECIPIENT_CONFLICT/);
assert.throws(()=>assertMaintenanceClear({lockHeld:true,workerSessions:'0'}),/WORKER_OR_WRITER_STILL_ACTIVE/);
assert.throws(()=>assertMaintenanceClear({lockHeld:false,workerSessions:'1'}),/WORKER_OR_WRITER_STILL_ACTIVE/);
assert.equal(assertMaintenanceClear({lockHeld:false,workerSessions:'0'}),true);

function fakeDb(initialRow,{drift=false,failUpdate=false}={}){
  let row=structuredClone(initialRow),rolledBack=false,committed=false;
  return {get row(){return row;},get rolledBack(){return rolledBack;},get committed(){return committed;},async query(sql,values=[]){
    if(sql==='BEGIN')return {rows:[]};if(sql==='COMMIT'){committed=true;return {rows:[]};}if(sql==='ROLLBACK'){rolledBack=true;return {rows:[]};}
    if(sql.includes('FOR UPDATE')){if(drift)row={...row,version:String(Number(row.version)+1)};return {rows:[structuredClone(row)]};}
    if(sql.includes('UPDATE relic_market_indexer.checkpoints')){if(failUpdate)throw Error('simulated compare conflict');row={...row,manifest:JSON.parse(values[1]),manifest_digest:values[2]};return {rows:[structuredClone(row)]};}
    throw Error(`unexpected sql: ${sql}`);
  }};
}
const db=fakeDb(cp()),snapshot=checkpointSnapshot(db.row);
const applied=await applyManifestMigration(db,{expected:snapshot,target:plan.target,digest:plan.targetDigest});
assert.equal(applied.changed,true);
assert.equal(db.committed,true);assert.equal(db.rolledBack,false);
assert.equal(db.row.manifest_digest,plan.targetDigest);
assert.equal(db.row.block_number,'110836342');assert.equal(db.row.block_hash,`0x${'2'.repeat(64)}`);assert.equal(db.row.version,'8880');
const repeated=await applyManifestMigration(db,{expected:checkpointSnapshot(db.row),target:plan.target,digest:plan.targetDigest});
assert.equal(repeated.changed,false,'repeated migration is a no-op');
assert.equal(db.row.version,'8880');
const reverse=priorManifest(db.row.manifest);
const rolledBack=await applyManifestMigration(db,{expected:checkpointSnapshot(db.row),target:reverse.target,digest:reverse.targetDigest,direction:'rollback'});
assert.equal(rolledBack.changed,true,'controlled rollback restores the recorded one-recipient manifest');
assert.equal(db.row.manifest_digest,plan.currentDigest);assert.equal(db.row.version,'8880');

const changedDb=fakeDb(cp(),{drift:true});
await assert.rejects(applyManifestMigration(changedDb,{expected:cp(),target:plan.target,digest:plan.targetDigest}),/CHECKPOINT_CHANGED_SINCE_PREFLIGHT/);
assert.equal(changedDb.rolledBack,true,'a changed checkpoint rolls back the complete migration');
const conflictDb=fakeDb(cp(),{failUpdate:true});
await assert.rejects(applyManifestMigration(conflictDb,{expected:cp(),target:plan.target,digest:plan.targetDigest}),/simulated compare conflict/);
assert.equal(conflictDb.rolledBack,true,'SQL conflict rolls back atomically');

process.stdout.write('manifest migration control tests passed\n');
