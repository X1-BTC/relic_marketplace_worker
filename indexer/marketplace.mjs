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
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    return Object.fromEntries(Object.keys(v).sort().map(k => [k, v[k]]));
  }
  return v;
});

export const copy = value => JSON.parse(json(value));
export const addr = value => address(value).toLowerCase();

export function hash(value) {
  if (!hashPattern.test(value)) throw Error('Invalid hash');
  return value;
}

export function integer(value) {
  if (!Number.isSafeInteger(value) || value < 0) throw Error('Unsafe block/log number');
  return value;
}

export function validateManifest(m = {}) {
  if (m.chainId !== 4663) throw Error('Robinhood Mainnet 4663 required');

  if (
    addr(m.nft) !== PRODUCTION_CONFIG.nftAddress ||
    addr(m.printer) !== PRODUCTION_CONFIG.currencyAddress
  ) {
    throw Error('Unauthorized NFT or PRINTER');
  }

  if (
    !Number.isInteger(m.printerDecimals) ||
    m.printerDecimals < 0 ||
    m.printerDecimals > 255
  ) {
    throw Error('Attested PRINTER decimals required');
  }

  addr(m.marketplace);

  if (new Set([m.nft, m.printer, m.marketplace].map(addr)).size !== 3) {
    throw Error('Distinct contracts required');
  }

  for (const key of ['nft', 'printer', 'marketplace']) {
    hash(m.codeHashes?.[key]);
  }

  for (const r of m.feeRecipients ?? []) {
    addr(r.address);
    if (!['platform', 'thirdweb', 'royalty'].includes(r.role)) {
      throw Error('Unknown fee role');
    }
  }

  if (
    new Set((m.feeRecipients ?? []).map(r => addr(r.address))).size !==
    (m.feeRecipients ?? []).length
  ) {
    throw Error('Duplicate fee recipient');
  }

  return copy(m);
}

export const manifestDigest = m => id(json(validateManifest(m)));

export function normalizeLog(log) {
  if (log.removed) throw Error('Removed log; recover canonical checkpoint first');

  const result = {
    address: addr(log.address),
    blockNumber: integer(Number(log.blockNumber)),
    blockHash: hash(log.blockHash),
    transactionHash: hash(log.transactionHash),
    logIndex: integer(log.logIndex),
    topics: log.topics.map(hash),
    data: log.data
  };

  if (!/^0x(?:[0-9a-fA-F]{2})*$/.test(result.data)) {
    throw Error('Invalid log data');
  }

  if (log.transactionIndex !== undefined && log.transactionIndex !== null) {
    result.transactionIndex = integer(log.transactionIndex);
  }

  result.blockTimestamp = log.blockTimestamp == null
    ? null
    : new Date(log.blockTimestamp).toISOString();

  return result;
}

export const eventKey = e => `${e.transactionHash}:${e.logIndex}`;
export const order = (a, b) => a.blockNumber - b.blockNumber || a.logIndex - b.logIndex;

export function decode(log, m) {
  if (log.address === addr(m.marketplace)) {
    const parsed = marketEvents.parseLog(log);
    if (!parsed) return null;

    const args = {};

    parsed.fragment.inputs.forEach((input, i) => {
      args[input.name] = input.baseType === 'tuple'
        ? Object.fromEntries(
            input.components.map((c, j) => [c.name, copy(parsed.args[i][j])])
          )
        : copy(parsed.args[i]);
    });

    return { name: parsed.name, args };
  }

  if (log.topics[0] !== id('Transfer(address,address,uint256)')) {
    return null;
  }

  const isNft = log.address === addr(m.nft);

  if (!isNft && log.address !== addr(m.printer)) {
    throw Error('Unauthorized log address');
  }

  const p = (isNft ? nftTransfer : transfer).parseLog(log);

  return {
    name: isNft ? 'NFTTransfer' : 'PrinterTransfer',
    args: {
      from: p.args.from.toLowerCase(),
      to: p.args.to.toLowerCase(),
      value: p.args[2].toString()
    }
  };
}

function checkRow(row, m) {
  if (
    addr(row.assetContract) !== addr(m.nft) ||
    addr(row.currency) !== addr(m.printer)
  ) {
    throw Error('Unsupported asset/currency');
  }

  for (const key of [
    'tokenId',
    'quantity',
    'pricePerToken',
    'totalPrice',
    'startTimestamp',
    'endTimestamp',
    'expirationTimestamp'
  ]) {
    if (row[key] !== undefined) tokenId(row[key]);
  }

  if (
    row.quantity !== '1' ||
    row.tokenType !== '0' ||
    row.status !== '1'
  ) {
    throw Error('Only active ERC721 single-token records supported');
  }
}

export function project(events, manifest, seed = null) {
  const m = validateManifest(manifest);

  const state = {
    listings: copy(seed?.listings ?? {}),
    offers: copy(seed?.offers ?? {}),
    sales: {},
    transfers: {}
  };

  const seen = new Map();

  for (const e of [...events].sort(order)) {
    if (![m.marketplace, m.nft, m.printer].map(addr).includes(e.address)) {
      throw Error('Unauthorized log address');
    }

    const key = eventKey(e);

    if (seen.has(key)) {
      if (seen.get(key) !== json(e)) {
        throw Error('Conflicting duplicate log');
      }
      continue;
    }

    seen.set(key, json(e));

    const d = decode(e, m);
    if (!d) continue;

    const a = d.args;

    if ([
      'PlatformFeeInfoUpdated',
      'FlatPlatformFeeUpdated',
      'PlatformFeeTypeUpdated',
      'RoyaltyEngineUpdated'
    ].includes(d.name)) {
      continue;
    }

    if (d.name.endsWith('Transfer')) {
      state.transfers[key] = {
        ...a,
        kind: d.name,
        transactionHash: e.transactionHash,
        logIndex: e.logIndex
      };
      continue;
    }

    const isOffer = d.name.includes('Offer');
    const records = isOffer ? state.offers : state.listings;
    const recordId = String(isOffer ? a.offerId : a.listingId);
    const prior = records[recordId];

    if (['NewListing', 'UpdatedListing', 'NewOffer'].includes(d.name)) {
      const row = copy(a.listing ?? a.offer);
      checkRow(row, m);

      if (
        String(row[isOffer ? 'offerId' : 'listingId']) !== recordId ||
        addr(row.assetContract) !== addr(a.assetContract) ||
        addr(row[isOffer ? 'offeror' : 'listingCreator']) !==
          addr(a[isOffer ? 'offeror' : 'listingCreator'])
      ) {
        throw Error('Event tuple mismatch');
      }

      if (d.name === 'UpdatedListing') {
        if (!prior || prior.status !== '1') {
          throw Error('Missing/closed listing before update');
        }

        if (
          row.tokenId !== prior.tokenId ||
          addr(row.assetContract) !== addr(prior.assetContract) ||
          addr(row.listingCreator) !== addr(prior.listingCreator)
        ) {
          throw Error('Listing identity changed');
        }
      } else if (prior) {
        throw Error('Duplicate entity creation');
      }

      records[recordId] = {
        ...row,
        event: key,
        updates: (prior?.updates ?? 0) +
          (d.name === 'UpdatedListing' ? 1 : 0)
      };

      continue;
    }

    if (!prior || prior.status !== '1') {
      throw Error('Missing/closed entity; deployment history or verified seed required');
    }

    const creator = isOffer ? 'offeror' : 'listingCreator';

    if (addr(a[creator]) !== addr(prior[creator])) {
      throw Error('Creator mismatch');
    }

    if (d.name.startsWith('Cancelled')) {
      prior.status = '3';
      prior.event = key;
      continue;
    }

    if (['NewSale', 'AcceptedOffer'].includes(d.name)) {
      if (
        addr(a.assetContract) !== addr(m.nft) ||
        a.tokenId !== prior.tokenId ||
        a.quantityBought !== '1'
      ) {
        throw Error('Sale asset mismatch');
      }

      const total = tokenId(a.totalPricePaid);

      if (
        total !== tokenId(
          isOffer ? prior.totalPrice : prior.pricePerToken
        )
      ) {
        throw Error('Sale price mismatch');
      }

      prior.status = '2';
      prior.quantity = '0';
      prior.event = key;

      state.sales[key] = {
        kind: d.name,
        recordId,
        tokenId: a.tokenId,
        payer: addr(isOffer ? a.offeror : a.buyer),
        seller: addr(isOffer ?
