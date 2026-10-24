import {WorkerFault} from './worker-config.mjs';

// Keep readBatch/project/writer unchanged. Discovery scans MARKETPLACE only.
// readBatch then obtains the complete receipt and retains the authorized NFT /
// PRINTER logs as settlement evidence, not as a second ownership/Seaport feed.
export function marketplaceOnlyClient(base,manifest,metrics={action(){}}) {
  const call=(name,fn)=>async args=>{metrics.action(name);return fn(args);};
  return {
    getChainId:call('eth_chainId',async()=>{const id=await base.getChainId();if(id!==4663)throw new WorkerFault('RPC_CHAIN_MISMATCH',{fatal:true});return id;}),
    getBlock:call('eth_getBlockByNumber',args=>base.getBlock(args)),
    getBlockNumber:call('eth_blockNumber',()=>base.getBlockNumber({cacheTime:0})),
    getBytecode:call('eth_getCode',args=>base.getBytecode(args)),
    readContract:call('eth_call',args=>base.readContract(args)),
    getTransactionReceipt:call('eth_getTransactionReceipt',args=>base.getTransactionReceipt(args)),
    getLogs:call('eth_getLogs',args=>base.getLogs({...args,address:manifest.marketplace})),
  };
}
