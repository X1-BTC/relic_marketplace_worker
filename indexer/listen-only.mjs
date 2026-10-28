import { setTimeout as sleep } from 'node:timers/promises';
import { openMarketplaceLogSubscription } from './marketplace-ws.mjs';

// Observation only: no PostgreSQL, no HTTP RPC, no checkpoint, no backfill.
const address = '0xb87bbab928495bec8085eca758660790a0c67f9f';
const url = process.env.RM_ALCHEMY_WS_URL ?? process.env.ALCHEMY_WS_URL ?? process.env.RM_WS_RPC_URL;
if (!url) {
  console.error(JSON.stringify({ event: 'fatal', code: 'WS_URL_MISSING' }));
  process.exit(1);
}
const stop = new AbortController();
process.once('SIGTERM', () => stop.abort());
process.once('SIGINT', () => stop.abort());
const log = (event, extra = {}) => console.log(JSON.stringify({ time: new Date().toISOString(), event, ...extra }));
let attempt = 0;
while (!stop.signal.aborted) {
  let handle;
  try {
    handle = await openMarketplaceLogSubscription({
      url,
      address,
      signal: stop.signal,
      onLog: ({ removed }) => log('marketplace_log', { removed }),
      onState: state => log('ws_state', { state }),
    });
    attempt = 0;
    log('listening', { chainId: 4663, address });
    await handle.closed;
  } catch (error) {
    if (!stop.signal.aborted) log('ws_retry', { code: error?.code ?? 'WS_FAILURE' });
  } finally {
    try { await handle?.close(); } catch {}
  }
  if (!stop.signal.aborted) {
    attempt += 1;
    try { await sleep(Math.min(30000, 1000 * 2 ** Math.min(attempt, 5)), undefined, { signal: stop.signal }); } catch {}
  }
}
log('stopped');
