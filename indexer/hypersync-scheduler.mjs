const abortError=()=>Object.assign(new Error('HYPERSYNC_ABORTED'),{code:'HYPERSYNC_ABORTED'});
const abortableSleep=(ms,signal)=>new Promise((resolve,reject)=>{
  if(signal?.aborted){reject(abortError());return;}
  const timer=setTimeout(done,ms);
  function done(){clearTimeout(timer);signal?.removeEventListener('abort',aborted);resolve();}
  function aborted(){clearTimeout(timer);signal?.removeEventListener('abort',aborted);reject(abortError());}
  signal?.addEventListener('abort',aborted,{once:true});
});

// A module-level instance is shared by every HyperSync source in this worker.
// Slots are reserved when a transport attempt starts, so retries and pages
// consume the same rolling-window budget as first attempts.
export class HyperSyncRequestScheduler {
  constructor({limit=10,windowMs=60000,now=Date.now,sleep=abortableSleep,random=Math.random}={}){
    if(!Number.isInteger(limit)||limit<1||!Number.isFinite(windowMs)||windowMs<1)throw Error('Invalid HyperSync scheduler bounds');
    this.limit=limit;this.windowMs=windowMs;this.now=now;this.sleep=sleep;this.random=random;
    this.starts=[];this.blockedUntil=0;this.tail=Promise.resolve();
  }
  async request(task,signal){
    const previous=this.tail;
    let release;
    this.tail=new Promise(resolve=>{release=resolve;});
    await previous;
    try{
      while(true){
        if(signal?.aborted)throw abortError();
        const now=this.now();
        this.starts=this.starts.filter(value=>now-value<this.windowMs);
        const wait=Math.max(this.blockedUntil-now,this.starts.length>=this.limit?this.starts[0]+this.windowMs-now:0);
        if(wait>0){await this.sleep(wait,signal);continue;}
        this.starts.push(this.now());
        return await task();
      }
    }finally{release();}
  }
  defer(ms){if(Number.isFinite(ms)&&ms>0)this.blockedUntil=Math.max(this.blockedUntil,this.now()+ms);}
  backoff(attempt,{retryAfterMs=0}={}){
    const exponential=Math.min(60000,1000*2**Math.min(Math.max(0,attempt-1),6));
    const base=Math.max(retryAfterMs,exponential);
    const jitter=Math.floor(base*this.random()*0.2);
    const wait=base+jitter;
    this.defer(wait);
    return wait;
  }
}

export const sharedHyperSyncScheduler=new HyperSyncRequestScheduler();
