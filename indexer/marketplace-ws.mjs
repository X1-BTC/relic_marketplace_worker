const WS_HOST='robinhood-mainnet.g.alchemy.com';
const SUBSCRIBE_ID=1;
const fail=code=>Object.assign(new Error(code),{code});

function validateEndpoint(raw) {
  let url;
  try{url=new URL(raw);}catch{throw fail('WS_ENDPOINT_INVALID');}
  if(url.protocol!=='wss:'||url.hostname!==WS_HOST||url.username||url.password||url.search||url.hash||!/^\/v2\/[^/]+$/.test(url.pathname))throw fail('WS_ENDPOINT_INVALID');
  return url.href;
}

const messageText=async data=>{
  if(typeof data==='string')return data;
  if(data&&typeof data.text==='function')return data.text();
  if(data instanceof ArrayBuffer)return new TextDecoder().decode(data);
  if(ArrayBuffer.isView(data))return new TextDecoder().decode(data);
  return String(data);
};

/**
 * Opens exactly one eth_subscribe("logs", { address }) subscription. The
 * callback receives only a wake-up hint; payload fields are never forwarded to
 * the indexer or used as checkpoint evidence. Reconnection belongs to the
 * existing worker loop so every new subscription is followed by catch-up.
 */
export async function openMarketplaceLogSubscription({url,address,WebSocketImpl=globalThis.WebSocket,onLog=()=>{},onState=()=>{},metrics,timeoutMs=15000,signal}) {
  const endpoint=validateEndpoint(url);
  if(!/^0x[0-9a-fA-F]{40}$/.test(address??''))throw fail('WS_MARKETPLACE_ADDRESS_INVALID');
  if(typeof WebSocketImpl!=='function')throw fail('WS_RUNTIME_UNAVAILABLE');
  if(signal?.aborted)throw fail('WS_ABORTED');
  let socket;
  metrics?.wsConnectionAttempt?.();
  try{socket=new WebSocketImpl(endpoint);}catch{throw fail('WS_CONNECT_FAILED');}
  let subscriptionId=null,closed=false,settled=false,resolveClosed,rejectReady,resolveReady;
  const closedPromise=new Promise(resolve=>{resolveClosed=resolve;});
  const readyPromise=new Promise((resolve,reject)=>{resolveReady=resolve;rejectReady=reject;});
  const finish=(code='WS_CLOSED')=>{
    if(closed)return;closed=true;
    if(!settled){settled=true;rejectReady(fail(code));}
    if(socket.readyState===0||socket.readyState===1){try{socket.close();}catch{}}
    try{onState('disconnected');}catch{}
    resolveClosed(code);
  };
  const send=value=>{if(closed)throw fail('WS_CLOSED');socket.send(JSON.stringify(value));metrics?.wsRequestOut?.();};
  const onOpen=()=>{
    try{send({jsonrpc:'2.0',id:SUBSCRIBE_ID,method:'eth_subscribe',params:['logs',{address:address.toLowerCase()}]});}
    catch{finish('WS_SUBSCRIBE_SEND_FAILED');}
  };
  const onMessage=async event=>{
    metrics?.wsMessageIn?.();
    let parsed;
    try{parsed=JSON.parse(await messageText(event.data));}catch{return;}
    if(parsed?.id===SUBSCRIBE_ID){
      if(parsed.error||typeof parsed.result!=='string'||!/^0x[0-9a-fA-F]{64}$/.test(parsed.result)){finish('WS_SUBSCRIBE_REJECTED');return;}
      subscriptionId=parsed.result;settled=true;
      try{onState('connected');}catch{}
      resolveReady(handle);return;
    }
    const params=parsed?.params,result=params?.result;
    if(parsed?.method!=='eth_subscription'||params?.subscription!==subscriptionId||!result||typeof result!=='object')return;
    metrics?.wsNotification?.();
    // The address filter is repeated locally. All notification content remains
    // untrusted and is reduced to the removed flag used to trigger a canonical
    // HTTP checkpoint/reorg check.
    if(typeof result.address==='string'&&result.address.toLowerCase()===address.toLowerCase()){
      try{onLog({removed:result.removed===true});}catch{}
    }
  };
  const onError=()=>finish('WS_SOCKET_ERROR');
  const onClose=()=>finish('WS_SOCKET_CLOSED');
  socket.addEventListener('open',onOpen,{once:true});
  socket.addEventListener('message',onMessage);
  socket.addEventListener('error',onError,{once:true});
  socket.addEventListener('close',onClose,{once:true});
  const abort=()=>{try{socket.close();}catch{}finish('WS_ABORTED');};
  signal?.addEventListener('abort',abort,{once:true});
  const timer=setTimeout(()=>{try{socket.close();}catch{}finish('WS_CONNECT_TIMEOUT');},timeoutMs);
  if(signal?.aborted)abort();
  const handle={
    closed:closedPromise,
    async close(){
      if(closed)return;
      if(subscriptionId&&socket.readyState===1){try{send({jsonrpc:'2.0',id:SUBSCRIBE_ID+1,method:'eth_unsubscribe',params:[subscriptionId]});}catch{}}
      try{socket.close();}catch{}
      finish('WS_WORKER_STOP');
    },
  };
  try{return await readyPromise;}finally{clearTimeout(timer);signal?.removeEventListener('abort',abort);}
}
