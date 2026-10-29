# Relic Marketplace Worker 

Minimal standalone runtime package. Render uses Node.js 24, installs with `npm ci --omit=dev --ignore-scripts --no-audit --no-fund`, checks with `npm run check:marketplace-worker`, and starts with `node --max-old-space-size=192 indexer/worker.mjs`. Locally, `npm start` runs the same command.

## Safety state

- Keep `RM_WORKER_ENABLED=false`. Any value except the exact string `true` leaves the process disabled before PostgreSQL or RPC clients are opened.
- Mainnet chain ID is fixed to 4663. MarketplaceV3 must have a real deployed address and attested runtime manifest before activation; this package contains no fabricated address or manifest.
- Alchemy access is exclusively through `RM_MARKETPLACE_ALCHEMY_HTTP_URL` and `RM_MARKETPLACE_ALCHEMY_WS_URL` from the dedicated Marketplace Alchemy app (Account B). No legacy or frontend fallback is used. Keep endpoint values out of source and logs.
- PostgreSQL uses explicit `RM_DB_*` fields and verified TLS. Connection URLs and populated generic `PG*` variables are rejected when enabled. Transaction pooler is unsupported.
- Supabase Marketplace migrations are assumed to have been installed and verified separately. This package contains no migrations or deployment migration command; Render build and start do not execute SQL.
- Trading is not enabled by this worker. Keep it disabled until contract deployment, database permissions, manifest, and operational prerequisites are independently approved.

## Render variables

Required only after explicit activation: `RM_MARKETPLACE_MANIFEST`, `RM_INDEXER_STREAM`, `RM_DB_CONNECTION_MODE`, `RM_DB_HOST`, `RM_DB_USER`, `RM_DB_CA_CERT`, `RM_DB_NAME`, `RM_DB_PASSWORD`, `RM_MARKETPLACE_ALCHEMY_HTTP_URL`, and `RM_MARKETPLACE_ALCHEMY_WS_URL`. Optional validated tuning variables: `RM_DB_PORT`, `RM_POLL_INTERVAL_MS`, `RM_BATCH_SIZE`, `RM_CONFIRMATIONS`, `RM_WS_FALLBACK_INTERVAL_MS`, `RM_WS_RECONNECT_MIN_MS`, `RM_WS_RECONNECT_MAX_MS`, `RM_CATCHUP_BATCH_DELAY_MS`, `RM_ALERT_LAG_BLOCKS`, `RM_ALERT_STALL_MS`, `RM_RETRY_MIN_MS`, `RM_RETRY_MAX_MS`, and `RM_SHUTDOWN_TIMEOUT_MS`. See `indexer/worker-config.mjs` for exact validation and defaults. Do not define `DATABASE_URL` or generic `PG*` overrides.

## Local checks

Run `npm ci`, `npm run build`, then `RM_WORKER_ENABLED=false npm start`. The build performs static and syntax checks only; these commands do not connect to Alchemy, PostgreSQL, or Supabase.
