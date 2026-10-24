import {integer, readBatch, verifyChain} from './marketplace.mjs';

// Explicit invocation only. The caller supplies a viem read client and a local
// PostgreSQL writer. No timer, environment lookup, deployment or auto migration.
export async function indexOnce({client, writer, confirmations=12, batchSize=100, checkpoint:knownCheckpoint=null, head:knownHead=null, verifyIdle=true}) {
  integer(confirmations);
  integer(batchSize);
  if (confirmations < 1 || batchSize < 1 || batchSize > 1000) throw Error('Invalid indexer bounds');
  const checkpoint=knownCheckpoint??await writer.checkpoint();
  const start=integer(Number(checkpoint.block_number));
  const anchor=await client.getBlock({blockNumber:BigInt(start)});
  if(anchor.hash!==checkpoint.block_hash)throw Error('Reorg: explicit recovery required');
  const head=knownHead??await client.getBlockNumber();
  const confirmed=head-BigInt(confirmations);
  if (confirmed <= BigInt(start)) {
    if(verifyIdle)await verifyChain(client,writer.manifest,start);
    return {idle:true,checkpoint};
  }
  const end=confirmed < BigInt(start+batchSize) ? Number(confirmed) : start+batchSize;
  const batch=await readBatch({client,manifest:writer.manifest,checkpoint,toBlock:end,confirmations,head,anchor});
  return writer.commit(batch);
}
