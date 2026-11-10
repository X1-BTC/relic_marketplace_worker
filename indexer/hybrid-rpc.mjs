const unsupportedMethod=error=>{
  for(let current=error;current;current=current.cause){
    if(current.code===-32601||current.code==='METHOD_NOT_FOUND')return true;
    if(/method (?:not found|not supported|does not exist)|unsupported method/i.test(String(current.shortMessage??current.message??'')))return true;
  }
  return false;
};

export class RpcFallbackScheduler {
  constructor({limit=10,windowMs=60000,now=Date.now,sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms))}={}){
    if(!Number.isInteger(limit)||limit<1||!Number.isFinite(windowMs)||windowMs<1)throw Error('Invalid RPC fallback bounds');
    this.limit=limit;this.windowMs=windowMs;this.now=now;this.sleep=sleep;this.starts=[];this.tail=Promise.resolve();
  }
  async request(task){
    const previous=this.tail;let release;this.tail=new Promise(resolve=>{release=resolve;});await previous;
    try{
      while(true){
        const now=this.now();this.starts=this.starts.filter(time=>now-time<this.windowMs);
        if(this.starts.length<this.limit){this.starts.push(this.now());return await task();}
        await this.sleep(Math.max(1,this.starts[0]+this.windowMs-now));
      }
    }finally{release();}
  }
}
const defaultFallbackScheduler=new RpcFallbackScheduler();

// HyperRPC is preferred for documented block and transaction reads. Any
// provider failure falls back to the rate-limited Alchemy read path; repeated
// failures are cooled down so an invalid HyperRPC token cannot stall indexing.
export function hybridReadClient({alchemy,envio,metrics={},onCapability=()=>{},now=Date.now,retryMs=60000,fallbackScheduler=defaultFallbackScheduler,rpcFacts=()=>({})}) {
  const methods=new Map();
  const run=async(method,args,envioRead,alchemyRead)=>{
    const state=methods.get(method);
    if(state?.unsupported)return fallback(method,args,alchemyRead);
    if(state?.unavailableUntil>now())return fallback(method,args,alchemyRead);
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
      onCapability(method,unsupported?'unsupported':'unavailable',rpcFacts());
      // HyperRPC is optional for these read-only methods. An auth, network, or
      // provider failure falls back to the existing Alchemy HTTP client so a
      // bad HyperRPC token cannot stall checkpointing. Calls through this path
      // are globally throttled; after a failure we skip HyperRPC for retryMs.
      return fallback(method,args,alchemyRead);
    }
  };
  const fallback=async(method,args,read)=>{
    return fallbackScheduler.request(async()=>{
      metrics.providerAction?.(method,'alchemy');
      try{return await read(args);}catch{throw Object.assign(new Error('RPC_READ_FAILED'),{code:'RPC_READ_FAILED'});}
    });
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

// Capture only transport status and JSON-RPC numeric error code. Never log or
// retain response bodies, request URLs, headers, API tokens, or error messages.
export function createRpcStatusFetch(onResponse=()=>{},fetchImpl=globalThis.fetch){
  if(typeof fetchImpl!=='function')throw Error('RPC_HTTP_UNAVAILABLE');
  let latest=null;
  const wrapped=async(...args)=>{
    latest=null;
    const response=await fetchImpl(...args);
    let jsonRpcCode;
    try{const body=await response.clone().json();if(Number.isInteger(body?.error?.code))jsonRpcCode=body.error.code;}catch{}
    latest={httpStatus:Number.isInteger(response.status)?response.status:undefined,jsonRpcCode};
    onResponse(latest);
    return response;
  };
  wrapped.facts=()=>latest??{};
  return wrapped;
}
