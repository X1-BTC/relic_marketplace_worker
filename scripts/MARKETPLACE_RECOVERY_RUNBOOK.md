# MarketplaceV3 targeted recovery runbook

These commands are prepared, not authorized or executed against production. They use only the eleven receipt hashes embedded in `audit-marketplace-history.mjs`; they do not run a historical `eth_getLogs` range scan. The only write commands are explicitly gated.

## A. Local prevalidation

From the worker repository:

```sh
node scripts/prevalidate-marketplace-recovery.mjs
```

For read-only RPC/database checks from an environment carrying the existing Render variables:

```sh
node scripts/prevalidate-marketplace-recovery.mjs --online-read-only
```

The online option runs the manifest dry-run, targeted receipts audit and replay dry-run. It never obtains the writer advisory lock or writes rows.

## B. Maintenance and manifest

1. Stop every Render instance running this worker. Keep it stopped until the manifest migration and history replay are complete.
2. In the Render service environment, retain the existing manifest as a rollback copy. The read-only preflight also prints `beforeManifest` and `targetManifest`; retain that output securely with the maintenance record. No database password, RPC URL or token is printed.
3. Run the read-only manifest preflight:

   ```sh
   node scripts/migrate-marketplace-manifest.mjs --dry-run
   ```

   Confirm `advisoryLockHeldBeforeMigration:false`, `otherNamedWorkerSessions:"0"`, the expected two recipients, and verified contract hashes. Update only `RM_MARKETPLACE_MANIFEST` in Render to the emitted `targetManifest`, preserving all code hashes and other fields. Do not start the worker.
4. Re-run the dry-run and require `renderManifestAligned:true`.
5. After explicit production authorization, run the gated migration:

   ```sh
   RM_ALLOW_PRODUCTION_MANIFEST_MIGRATION=true node scripts/migrate-marketplace-manifest.mjs --apply --maintenance-confirmation=STOPPED_AND_READY
   ```

   The script takes the worker’s advisory lock, locks the checkpoint row, compares the live row with the preflight snapshot, and updates only `manifest` and `manifest_digest`. It leaves `block_number`, `block_hash`, anchors and `version` unchanged. A changed checkpoint, held lock, active named worker, or digest mismatch aborts and rolls back.

## C. Targeted audit and replay

With Render’s manifest set to the new manifest and the worker still stopped:

```sh
node scripts/audit-marketplace-history.mjs --targeted
node scripts/replay-marketplace-history.mjs --dry-run
```

Review all receipt identities, decoded event counts, missing database identities, the sale `#1509` evidence, and the deterministic replay batches. The sale must show seller 8.55, platform 0.405, Thirdweb 0.045 and gross 9 PRINTER (all raw amounts at 18 decimals), with the NFT transferred from the seller to the buyer. The pre-replay database comparison is expected to report missing rows; the projected settlement must be reconciled. If replay readiness is false, do not proceed.

Record the live checkpoint version shown in the audit, then, only after explicit production authorization:

```sh
RM_ALLOW_PRODUCTION_MARKETPLACE_REPLAY=true node scripts/replay-marketplace-history.mjs --apply --maintenance-confirmation=STOPPED_AND_READY --expected-checkpoint-version=<reviewed-live-version>
```

Replay re-audits after acquiring the advisory lock, verifies confirmations and canonical block hashes immediately before each block transaction, and calls the existing `PostgresMarketplaceWriter.replayHistorical()`. Each event block commits transactionally; a later block failure leaves earlier blocks committed and safe to resume idempotently. It never rewinds or advances the checkpoint. A later transition for a target listing or offer is a hard conflict and rolls back that event-block transaction.

## D. Restart and validation

After replay reports zero missing targeted identities and exact database settlement/transfer agreement:

1. Start exactly one Render worker instance.
2. Confirm `LEADER`, WebSocket connected, Envio ready, no manifest mismatch, and checkpoint progress in Render logs.
3. Re-run `node scripts/audit-marketplace-history.mjs --targeted` read-only. Require zero missing identities and `financialValidation.allSalesMatch:true`.
4. Confirm the 15 expected receipt identities across the targeted receipts, and the known sale’s `NewSale` settlement at log index 4 with `RECONCILED`, 3 PRINTER payment transfers and the NFT transfer. The targeted audit output already performs those comparisons; no SQL writes are needed.

## E. Controlled rollback

Rollback is only for a stopped worker before subsequent business changes are relied upon. Restore the exact recorded `beforeManifest` in Render first, keep the worker stopped, run the dry-run, and require `renderManifestAligned:true`. After explicit authorization:

```sh
RM_ALLOW_PRODUCTION_MANIFEST_ROLLBACK=true node scripts/migrate-marketplace-manifest.mjs --rollback --apply --maintenance-confirmation=STOPPED_AND_READY
```

This changes only checkpoint manifest metadata and digest back to the prior one-recipient manifest. It does not reverse replayed event rows or settlements. If replay has completed, rolling back the manifest is not a data rollback and may make future fee reconciliation ambiguous; prefer forward repair unless an owner-approved recovery plan says otherwise.

## Operational boundaries

- Do not run `--full` on the history audit for this repair.
- Do not reset or edit the checkpoint position/version, delete rows, run SQL inserts manually, or modify schema.
- The migration and replay `--apply` flags require explicit environment approval plus maintenance confirmation; they have not been run here.
- The worker’s advisory lock fences compliant workers. The operator must still stop all Render instances and confirm no separate writer is active.
