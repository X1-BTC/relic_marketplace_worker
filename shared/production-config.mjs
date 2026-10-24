export const PRODUCTION_CONFIG = Object.freeze({
  chainId: 4663,
  networkName: 'Robinhood Mainnet',
  nftAddress: '0xb509e195bcb3e4461e235ff152c68d66915f67b5',
  currencyName: 'PRINTER',
  currencyAddress: '0x85a574f2ff0795685f58d1d7b0d4b51f148ac489',
  currencyDecimals: 18,
  rpcFallback: 'https://rpc.mainnet.chain.robinhood.com',
  marketplaceAddress: null,
  transactionSubmissionEnabled: false,
});

export function validateProductionConfig(config = PRODUCTION_CONFIG) {
  return config.chainId === 4663
    && config.networkName === 'Robinhood Mainnet'
    && config.nftAddress.toLowerCase() === '0xb509e195bcb3e4461e235ff152c68d66915f67b5'
    && config.currencyName === 'PRINTER'
    && config.currencyAddress.toLowerCase() === '0x85a574f2ff0795685f58d1d7b0d4b51f148ac489'
    && config.currencyDecimals === 18
    && config.rpcFallback === 'https://rpc.mainnet.chain.robinhood.com'
    && config.marketplaceAddress === null
    && config.transactionSubmissionEnabled === false;
}
