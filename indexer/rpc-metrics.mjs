const methods=['eth_blockNumber','eth_chainId','eth_getBlockByNumber','eth_getTransactionReceipt','eth_getCode','eth_call'];

// Counts client actions separately from transport requests. No request URL,
// body, response, transaction hash, or provider diagnostic is retained.
export function createRpcMetrics() {
  const actions=Object.fromEntries(methods.map(method=>[method,0]));
  let httpFetchCalls=0,wsConnectionAttempts=0,wsRequestsOut=0,wsMessagesIn=0,wsNotifications=0;
  let envioRpcCalls=0,envioRpcFailures=0,alchemyRpcCalls=0,alchemyEstimatedCu=0,alchemyUnpricedCalls=0;
  let hypersyncRequests=0,hypersyncPages=0,hypersyncFailures=0,hypersyncRangeFailures=0,hypersyncRetries=0,hypersyncBlocksVerified=0,hypersyncLogs=0;
  let heapUsedBytes=0,rssBytes=0,peakHeapUsedBytes=0,peakRssBytes=0;
  return {
    action(method){if(Object.hasOwn(actions,method))actions[method]++;},
    providerAction(method,provider){
      if(provider==='envio')envioRpcCalls++;
      else if(provider==='envioFailure'){envioRpcFailures++;}
      else if(provider==='alchemy'){
        alchemyRpcCalls++;
        if(method==='eth_blockNumber')alchemyEstimatedCu+=10;
        else if(method==='eth_getBlockByNumber')alchemyEstimatedCu+=20;
        else alchemyUnpricedCalls++;
      }
    },
    httpFetchCall(){httpFetchCalls++;},
    request(){hypersyncRequests++;},
    page(){hypersyncPages++;},
    failure(){hypersyncFailures++;},
    rangeFailure(){hypersyncRangeFailures++;},
    retry(){hypersyncRetries++;},
    verifiedRange(blocks,logs){hypersyncBlocksVerified+=blocks;hypersyncLogs+=logs;},
    memorySample(heap,rss){heapUsedBytes=heap;rssBytes=rss;peakHeapUsedBytes=Math.max(peakHeapUsedBytes,heap);peakRssBytes=Math.max(peakRssBytes,rss);},
    wsConnectionAttempt(){wsConnectionAttempts++;},
    wsRequestOut(){wsRequestsOut++;},
    wsMessageIn(){wsMessagesIn++;},
    wsNotification(){wsNotifications++;},
    snapshot({reset=false}={}){
      const result={...actions,httpFetchCalls,wsConnectionAttempts,wsRequestsOut,wsMessagesIn,wsNotifications,
        envioRpcCalls,envioRpcFailures,alchemyRpcCalls,alchemyEstimatedCu,alchemyUnpricedCalls,
        hypersyncRequests,hypersyncPages,hypersyncFailures,hypersyncRangeFailures,hypersyncRetries,hypersyncBlocksVerified,hypersyncLogs,
        heapUsedBytes,rssBytes,peakHeapUsedBytes,peakRssBytes};
      if(reset){
        for(const method of methods)actions[method]=0;
        httpFetchCalls=0;wsConnectionAttempts=0;wsRequestsOut=0;wsMessagesIn=0;wsNotifications=0;
        envioRpcCalls=0;envioRpcFailures=0;alchemyRpcCalls=0;alchemyEstimatedCu=0;alchemyUnpricedCalls=0;
        hypersyncRequests=0;hypersyncPages=0;hypersyncFailures=0;hypersyncRangeFailures=0;hypersyncRetries=0;hypersyncBlocksVerified=0;hypersyncLogs=0;
        peakHeapUsedBytes=heapUsedBytes;peakRssBytes=rssBytes;
      }
      return result;
    },
  };
}

export function createCountingFetch(metrics,fetchImpl=globalThis.fetch) {
  if(typeof fetchImpl!=='function')throw Error('HTTP transport unavailable');
  return (...args)=>{metrics.httpFetchCall();return fetchImpl(...args);};
}
