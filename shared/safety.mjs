export const MAINNET_CHAIN_ID = 4663;
export const MAINNET_RPC = '/rpc';
export const MAX_UINT256 = (1n << 256n) - 1n;
export const same = (a, b) => !!a && !!b && a.toLowerCase() === b.toLowerCase();
export function assertChain(value) {
  if (Number(value) !== MAINNET_CHAIN_ID) throw new Error('Wrong chain: Robinhood Mainnet 4663 is required. Operation blocked.');
}
export function assertWalletContext(expectedChain, currentChain, expectedAddress, currentAddress) {
  assertChain(expectedChain);
  assertChain(currentChain);
  if (!same(expectedAddress, currentAddress)) throw new Error('Wallet account changed before broadcast. Review and retry.');
}
export function address(value) {
  if (!/^0x[0-9a-fA-F]{40}$/.test(value || '') || /^0x0{40}$/.test(value)) throw new Error('A nonzero public contract address is required.');
  return value;
}
export function tokenId(value) {
  if (typeof value !== 'string' && typeof value !== 'bigint' && typeof value !== 'number') throw new Error('Token ID must be a uint256 integer.');
  if (typeof value === 'number' && (!Number.isSafeInteger(value) || Object.is(value, -0))) throw new Error('Token ID number is unsafe; pass an exact decimal string or bigint.');
  const text = typeof value === 'bigint' ? value.toString() : String(value);
  if (!/^(0|[1-9]\d*)$/.test(text) || BigInt(text) > MAX_UINT256) throw new Error('Token ID must be a uint256 integer.');
  return BigInt(text);
}
export function price(value, decimals = 18) {
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 36) throw new Error('Invalid token decimals.');
  if (typeof value !== 'string' || !/^(0|[1-9]\d*)(\.\d+)?$/.test(value)) throw new Error('Enter a positive decimal price.');
  const [whole, fraction = ''] = value.split('.');
  if (fraction.length > decimals) throw new Error(`At most ${decimals} decimal places are allowed.`);
  const result = BigInt(whole) * 10n ** BigInt(decimals) + BigInt(fraction.padEnd(decimals, '0') || '0');
  if (result <= 0n || result > MAX_UINT256) throw new Error('Price is outside the supported range.');
  return result;
}
export function duration(value) {
  if (typeof value !== 'number' && (typeof value !== 'string' || !/^(0|[1-9]\d*)$/.test(value))) throw new Error('Duration must be an integer number of seconds.');
  if (typeof value === 'number' && (!Number.isSafeInteger(value) || Object.is(value, -0))) throw new Error('Duration must be an integer number of seconds.');
  const seconds = Number(value);
  if (!Number.isInteger(seconds) || seconds < 60 || seconds > 30 * 86400) throw new Error('Duration must be 60 seconds to 30 days.');
  return seconds;
}
export function created(status) { return status === 1 || status === 1n || ['created', 'active'].includes(String(status).toLowerCase()) || status === '1'; }
export function offerState(offer, funds, now, groupFilled = false) {
  const status = String(offer.status).toLowerCase();
  if (status === 'completed' || status === '2') return 'ACCEPTED';
  if (status === 'cancelled' || status === '3') return 'CANCELLED';
  if (status === 'expired') return 'EXPIRED';
  if (!created(offer.status)) return 'UNKNOWN';
  if (BigInt(offer.endTimeInSeconds) <= BigInt(now)) return 'EXPIRED';
  if (!funds) return 'UNKNOWN';
  if (funds.balance < BigInt(offer.totalPrice) || funds.allowance < BigInt(offer.totalPrice)) return groupFilled ? 'GROUP_FILLED / UNFUNDED' : 'UNFUNDED';
  return 'ACTIVE';
}
// Reject partial results; scan the unfiltered ID space, not only valid offers.
export async function scanAll({ total, page, pageSize = 100, maxRecords = 100000, metrics = {} }) {
  const count = BigInt(await total());
  if (count < 0n || count > BigInt(maxRecords)) throw new Error('Incomplete scan: record safety limit exceeded.');
  const rows = []; metrics.pages = 0; metrics.records = 0;
  for (let start = 0n; start < count; start += BigInt(pageSize)) {
    const size = Number(count - start < BigInt(pageSize) ? count - start : BigInt(pageSize));
    const next = await page(Number(start), BigInt(size));
    metrics.pages++;
    if (next.length !== size) throw new Error('Incomplete scan: missing records. Approval blocked.');
    for (let i = 0; i < next.length; i++) if (BigInt(next[i].id) !== start + BigInt(i)) throw new Error('Incomplete scan: unexpected record IDs.');
    rows.push(...next);
  }
  if (BigInt(await total()) !== count) throw new Error('Marketplace changed during scan. Retry before approval.');
  metrics.records = rows.length;
  return rows;
}
export function riskyOffers(offers, owner, currency, amount, now) {
  return offers.filter(o => same(o.offerorAddress, owner) && same(o.currencyContractAddress, currency) && created(o.status) && BigInt(o.endTimeInSeconds) > BigInt(now) && BigInt(o.totalPrice) <= amount);
}
export function assertApprovalScan(scan, acknowledged, expectedRiskIds = []) {
  if (!scan.complete) throw new Error('Complete stale-offer discovery is required before approval.');
  const ids = scan.risks.map(o => String(o.id)).sort();
  if (ids.length && (!acknowledged || ids.join(',') !== [...expectedRiskIds].sort().join(','))) throw new Error('Old offers may reactivate. Review the current complete scan before approving.');
}
export function assertListing(live, expected, owner, now) {
  for (const key of ['id', 'tokenId', 'pricePerToken', 'quantity', 'startTimeInSeconds', 'endTimeInSeconds']) {
    if (String(live[key]) !== String(expected[key])) throw new Error('Listing changed. Refresh and review before buying.');
  }
  for (const key of ['assetContractAddress', 'currencyContractAddress', 'creatorAddress']) if (!same(live[key], expected[key])) throw new Error('Listing changed.');
  if (!created(live.status) || !same(owner, live.creatorAddress) || live.quantity !== 1n || live.isReservedListing || BigInt(live.startTimeInSeconds) > BigInt(now) || BigInt(live.endTimeInSeconds) <= BigInt(now)) throw new Error('Listing is no longer safely executable.');
}
export function bulkIds(ids) {
  if (!ids.length || ids.length > 50) throw new Error('Select 1–50 Machines per batch.');
  const parsed = ids.map(tokenId);
  if (new Set(parsed.map(String)).size !== parsed.length) throw new Error('Duplicate Machine selected.');
  return parsed;
}
export async function mapLimit(items, limit, fn) {
  const results = new Array(items.length); let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) { const i = cursor++; if (i >= items.length) break; results[i] = await fn(items[i], i); }
  }));
  return results;
}
