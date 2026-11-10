const ENDPOINT='https://robinhood.hypersync.xyz/query';
const MARKETPLACE='0xb87bbab928495bec8085eca758660790a0c67f9f';
const LOG_FIELDS=['address','block_number','block_hash','transaction_hash','transaction_index','log_index','topic0','topic1','topic2','topic3','data','removed'];
const BLOCK_FIELDS=['number','hash','parent_hash'];

const fail=code=>Object.assign(new Error(code),{code});
const num=(v,name)=>{
  const n=typeof v==='string'&&/^0x[0-9a-f]+$/i.test(v)?Number(BigInt(v)):Number(v);
  if(!Number.isSafeInteger(n)||n<0)throw fail(`HYPERSYNC_${name}_INVALID`);
  return n;
};
const hash=v=>typeof v==='string'&&/^0x[0-9a-f]{64}$/i.test(v);

export class HyperSyncLogSource {
  constructor({apiToken,fetchImpl=globalThis.fetch,metrics={},signal,timeoutMs=60000,onReady=()=>{}}){
    if(typeof apiToken!=='string'||!apiToken.trim())throw fail('HYPERSYNC_TOKEN_REQUIRED');
    if(typeof fetchImpl!=='function')throw fail('HYPERSYNC_HTTP_UNAVAILABLE');
    this.apiToken=apiToken;this.fetchImpl=fetchImpl;this.metrics=metrics;this.signal=signal;this.timeoutMs=timeoutMs;this.onReady=onReady;this.ready=false;
  }
  async request(fromBlock,toBlock){
    const controller=new AbortController();
    const timer=setTimeout(()=>controller.abort(),this.timeoutMs);timer.unref?.();
    const signal=this.signal?AbortSignal.any([this.signal,controller.signal]):controller.signal;
    try{
      this.metrics.request?.();
      const response=await this.fetchImpl(ENDPOINT,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${this.apiToken}`},signal,
        body:JSON.stringify({from_block:fromBlock,to_block:toBlock,include_all_blocks:true,logs:[{address:[MARKETPLACE]}],field_selection:{block:BLOCK_FIELDS,log:LOG_FIELDS}})});
      if(!response?.ok)throw fail('HYPERSYNC_REQUEST_FAILED');
      const body=await response.json();
      return body;
    }catch(error){
      if(error?.code?.startsWith('HYPERSYNC_'))throw error;
      throw fail(this.signal?.aborted?'HYPERSYNC_ABORTED':'HYPERSYNC_REQUEST_FAILED');
    }finally{clearTimeout(timer);}
  }
  async getLogs({address,fromBlock,toBlock}){
    if(typeof address!=='string'||address.toLowerCase()!==MARKETPLACE)throw fail('HYPERSYNC_ADDRESS_INVALID');
    let cursor=num(fromBlock,'CURSOR'),end=num(toBlock,'END');
    if(end<cursor)throw fail('HYPERSYNC_RANGE_INVALID');
    if(end-cursor>1000)throw fail('HYPERSYNC_RANGE_TOO_LARGE');
    const logs=[],headers=[],guards=[];
    while(cursor<end){
      const body=await this.request(cursor,end);
      const next=num(body?.next_block,'CURSOR');
      if(next<=cursor||next>end)throw fail('HYPERSYNC_CURSOR_INVALID');
      if(!Array.isArray(body?.data))throw fail('HYPERSYNC_DATA_INVALID');
      const guard=body.rollback_guard;
      if(guard!=null){
        const g={blockNumber:num(guard.block_number,'GUARD'),hash:guard.hash,firstBlockNumber:num(guard.first_block_number,'GUARD'),firstParentHash:guard.first_parent_hash};
        if(!hash(g.hash)||!hash(g.firstParentHash)||g.firstBlockNumber<cursor||g.blockNumber<g.firstBlockNumber||g.blockNumber>=next)throw fail('HYPERSYNC_GUARD_INVALID');
        const prior=guards.at(-1);
        if(prior&&g.firstParentHash.toLowerCase()!==prior.hash.toLowerCase())throw fail('HYPERSYNC_REORG_DETECTED');
        guards.push(g);
      }
      for(const chunk of body.data){
        if(Array.isArray(chunk?.blocks))for(const raw of chunk.blocks){
          const number=num(raw.number,'BLOCK_NUMBER');
          if(number<cursor||number>=next||number>=end||!hash(raw.hash)||!hash(raw.parent_hash))throw fail('HYPERSYNC_BLOCK_INVALID');
          headers.push({number,hash:raw.hash.toLowerCase(),parentHash:raw.parent_hash.toLowerCase()});
        }
        if(!Array.isArray(chunk?.logs))continue;
        for(const raw of chunk.logs){
          const blockNumber=num(raw.block_number,'LOG_BLOCK');
          const logIndex=num(raw.log_index,'LOG_INDEX');
          const transactionIndex=num(raw.transaction_index,'LOG_TX_INDEX');
          if(blockNumber<cursor||blockNumber>=next||blockNumber>=end||typeof raw.address!=='string'||raw.address.toLowerCase()!==MARKETPLACE||!hash(raw.block_hash)||!hash(raw.transaction_hash)||typeof raw.data!=='string'||!/^0x(?:[0-9a-f]{2})*$/i.test(raw.data))throw fail('HYPERSYNC_LOG_INVALID');
          const topics=[];
          for(const key of ['topic0','topic1','topic2','topic3'])if(raw[key]!=null){if(!hash(raw[key]))throw fail('HYPERSYNC_TOPIC_INVALID');topics.push(raw[key].toLowerCase());}else break;
          if(!topics.length)throw fail('HYPERSYNC_TOPIC_INVALID');
          logs.push({address:raw.address.toLowerCase(),blockNumber,blockHash:raw.block_hash.toLowerCase(),transactionHash:raw.transaction_hash.toLowerCase(),transactionIndex,logIndex,topics,data:raw.data,removed:raw.removed===true});
        }
      }
      const pageHeaders=headers.filter(h=>h.number>=cursor&&h.number<next).sort((a,b)=>a.number-b.number);
      if(pageHeaders.length!==next-cursor)throw fail('HYPERSYNC_BLOCK_COVERAGE_INCOMPLETE');
      for(let i=0;i<pageHeaders.length;i++){
        if(pageHeaders[i].number!==cursor+i)throw fail('HYPERSYNC_BLOCK_COVERAGE_INCOMPLETE');
        if(i&&pageHeaders[i].parentHash!==pageHeaders[i-1].hash)throw fail('HYPERSYNC_REORG_DETECTED');
      }
      if(headers.length>pageHeaders.length&&pageHeaders[0].parentHash!==headers.at(-pageHeaders.length-1).hash)throw fail('HYPERSYNC_REORG_DETECTED');
      cursor=next;
    }
    const unique=new Map();
    for(const log of logs){
      const key=`${log.transactionHash}:${log.logIndex}`;
      const prior=unique.get(key);
      if(prior&&JSON.stringify(prior)!==JSON.stringify(log))throw fail('HYPERSYNC_DUPLICATE_CONFLICT');
      unique.set(key,log);
    }
    const ordered=[...unique.values()].sort((a,b)=>a.blockNumber-b.blockNumber||a.transactionIndex-b.transactionIndex||a.logIndex-b.logIndex);
    this.metrics.logs?.(ordered.length);
    if(!this.ready){this.ready=true;this.onReady();}
    return {logs:ordered,headers:headers.sort((a,b)=>a.number-b.number),rollbackGuards:guards};
  }
}

export const HYPERSYNC_ENDPOINT=ENDPOINT;
