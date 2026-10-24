const methods=['eth_blockNumber','eth_chainId','eth_getBlockByNumber','eth_getLogs','eth_getTransactionReceipt','eth_getCode','eth_call'];

// Counts client actions separately from transport requests. No request URL,
// body, response, transaction hash, or provider diagnostic is retained.
export function createRpcMetrics() {
  const actions=Object.fromEntries(methods.map(method=>[method,0]));
  let httpFetchCalls=0,wsConnectionAttempts=0,wsRequestsOut=0,wsMessagesIn=0,wsNotifications=0;
  return {
    action(method){if(Object.hasOwn(actions,method))actions[method]++;},
    httpFetchCall(){httpFetchCalls++;},
    wsConnectionAttempt(){wsConnectionAttempts++;},
    wsRequestOut(){wsRequestsOut++;},
    wsMessageIn(){wsMessagesIn++;},
    wsNotification(){wsNotifications++;},
    snapshot({reset=false}={}){
      const result={...actions,httpFetchCalls,wsConnectionAttempts,wsRequestsOut,wsMessagesIn,wsNotifications};
      if(reset){for(const method of methods)actions[method]=0;httpFetchCalls=0;wsConnectionAttempts=0;wsRequestsOut=0;wsMessagesIn=0;wsNotifications=0;}
      return result;
    },
  };
}

export function createCountingFetch(metrics,fetchImpl=globalThis.fetch) {
  if(typeof fetchImpl!=='function')throw Error('HTTP transport unavailable');
  return (...args)=>{metrics.httpFetchCall();return fetchImpl(...args);};
}
