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

// Canonical reads always use Alchemy and share one bounded request scheduler.
export function createAlchemyReadClient(alchemy,{metrics={},scheduler=defaultFallbackScheduler}={}) {
  const read=(method,fn)=>args=>scheduler.request(async()=>{
    metrics.providerAction?.(method,'alchemy');
    try{return await fn(args);}catch{throw Object.assign(new Error('RPC_READ_FAILED'),{code:'RPC_READ_FAILED'});}
  });
  return {
    getBlockNumber:read('eth_blockNumber',()=>alchemy.getBlockNumber()),
    getBlock:read('eth_getBlockByNumber',args=>alchemy.getBlock(args)),
    getTransactionReceipt:read('eth_getTransactionReceipt',args=>alchemy.getTransactionReceipt(args)),
    getChainId:read('eth_chainId',()=>alchemy.getChainId()),
    getBytecode:read('eth_getCode',args=>alchemy.getBytecode(args)),
    readContract:read('eth_call',args=>alchemy.readContract(args)),
  };
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
