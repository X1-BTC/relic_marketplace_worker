const unsupportedMethod=error=>{
  for(let current=error;current;current=current.cause){
    if(current.code===-32601||current.code==='METHOD_NOT_FOUND')return true;
    if(/method (?:not found|not supported|does not exist)|unsupported method/i.test(String(current.shortMessage??current.message??'')))return true;
  }
  return false;
};

// HyperRPC is used for documented block and transaction reads. Only an
// explicit unsupported-method response enables Alchemy fallback; transient
// failures stay visible to the worker retry path.
export function hybridReadClient({alchemy,envio,metrics={},onCapability=()=>{},now=Date.now,retryMs=60000}) {
  const methods=new Map();
  const run=async(method,args,envioRead,alchemyRead)=>{
    const state=methods.get(method);
    if(state?.unsupported)return fallback(method,args,alchemyRead);
    if(state?.unavailableUntil>now())throw Object.assign(new Error('RPC_READ_FAILED'),{code:'RPC_READ_FAILED'});
    try{
      const result=await envioRead(args);
      methods.set(method,{unsupported:false,unavailableUntil:0});
      metrics.providerAction?.(method,'envio');
      if(state?.unsupported!==false||state?.unavailableUntil)onCapability(method,'ready');
      return result;
    }catch(error){
      const unsupported=unsupportedMethod(error);
      methods.set(method,unsupported?{unsupported:true,unavailableUntil:0}:{unsupported:false,unavailableUntil:now()+retryMs});
      metrics.providerAction?.(method,'envioFailure');
      onCapability(method,unsupported?'unsupported':'unavailable');
      if(unsupported)return fallback(method,args,alchemyRead);
      throw Object.assign(new Error('RPC_READ_FAILED'),{code:'RPC_READ_FAILED'});
    }
  };
  const fallback=async(method,args,read)=>{
    metrics.providerAction?.(method,'alchemy');
    try{return await read(args);}catch{throw Object.assign(new Error('RPC_READ_FAILED'),{code:'RPC_READ_FAILED'});}
  };
  return {
    getBlockNumber:args=>run('eth_blockNumber',args??{cacheTime:0},envio.getBlockNumber.bind(envio),alchemy.getBlockNumber.bind(alchemy)),
    getBlock:args=>run('eth_getBlockByNumber',args,envio.getBlock.bind(envio),alchemy.getBlock.bind(alchemy)),
    getTransactionReceipt:args=>run('eth_getTransactionReceipt',args,envio.getTransactionReceipt.bind(envio),alchemy.getTransactionReceipt.bind(alchemy)),
    getChainId:args=>fallback('eth_chainId',args,alchemy.getChainId.bind(alchemy)),
    getBytecode:args=>fallback('eth_getCode',args,alchemy.getBytecode.bind(alchemy)),
    readContract:args=>fallback('eth_call',args,alchemy.readContract.bind(alchemy)),
  };
}

export function envioHyperRpcUrl(apiToken) {
  if(typeof apiToken!=='string'||!apiToken.trim())throw Error('ENVIO_TOKEN_REQUIRED');
  return `https://4663.rpc.hypersync.xyz/${encodeURIComponent(apiToken)}`;
}
