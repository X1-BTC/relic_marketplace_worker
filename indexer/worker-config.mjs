import {validateManifest} from './marketplace.mjs';

export const PROJECT_REF='colraxespxkbisfaazeo';
export class WorkerFault extends Error {
  constructor(code,{fatal=false}={}) {super(code);this.code=code;this.fatal=fatal;}
}
const invalid=()=>{throw new WorkerFault('CONFIG_INVALID',{fatal:true});};
const required=(env,key)=>typeof env[key]==='string'&&env[key].trim()?env[key]:invalid();
function number(env,key,fallback,min,max) {
  const raw=env[key] ?? String(fallback);
  if(!/^[0-9]+$/.test(raw))invalid();
  const value=Number(raw);if(!Number.isSafeInteger(value)||value<min||value>max)invalid();return value;
}
function alchemyEndpoint(env,key,protocol,missingCode) {
  const raw=env[key];
  if(typeof raw!=='string'||!raw.trim())throw new WorkerFault(missingCode,{fatal:true});
  let parsed;
  try{parsed=new URL(raw);}catch{invalid();}
  if(parsed.protocol!==protocol||parsed.hostname!=='robinhood-mainnet.g.alchemy.com'||parsed.username||parsed.password||parsed.search||parsed.hash||!/^\/v2\/[^/]+$/.test(parsed.pathname))invalid();
  return parsed;
}
export function readWorkerConfig(env) {
  // Importing this module has no side effects; secrets are only passed by the
  // explicit executable. Missing enablement does not construct any client.
  if(env.RM_WORKER_ENABLED!=='true')return {enabled:false};
  if(Object.keys(env).some(k=>k.startsWith('PG') && env[k]))invalid();
  // Explicit fields only: reject libpq environment overrides and connection URLs.
  if(env.DATABASE_URL || env.RM_DB_CONNECTION_URL)invalid();
  let manifest;
  try {manifest=validateManifest(JSON.parse(required(env,'RM_MARKETPLACE_MANIFEST')));}catch{invalid();}
  if(!Array.isArray(manifest.feeRecipients))invalid(); // explicit approved fee map, never an invented default
  const stream=required(env,'RM_INDEXER_STREAM');if(!/^[a-z0-9_-]{1,80}$/.test(stream))invalid();
  const mode=required(env,'RM_DB_CONNECTION_MODE');if(!['direct','session'].includes(mode))invalid();
  const host=required(env,'RM_DB_HOST');const user=required(env,'RM_DB_USER');
  const suffix='.'+PROJECT_REF;
  const login=mode==='session'?(user.endsWith(suffix)?user.slice(0,-suffix.length):invalid()):user;
  if(!/^[a-z_][a-z0-9_]{0,62}$/.test(login)||['postgres','service_role','anon','authenticated','authenticator','supabase_admin','relic_market_writer'].includes(login))invalid();
  if(mode==='direct' && host!==`db.${PROJECT_REF}.supabase.co`)invalid();
  if(mode==='session' && !/^aws-[a-z0-9-]+\.pooler\.supabase\.com$/.test(host))invalid();
  const port=number(env,'RM_DB_PORT',5432,5432,5432); // no transaction pooler port
  const ca=required(env,'RM_DB_CA_CERT');
  if(!ca.includes('-----BEGIN CERTIFICATE-----')||!ca.includes('-----END CERTIFICATE-----')||ca.includes('PRIVATE KEY'))invalid();
  const database=required(env,'RM_DB_NAME');if(!/^[a-zA-Z0-9_]+$/.test(database))invalid();
  const connection={host,port,database,user,password:required(env,'RM_DB_PASSWORD'),ssl:{rejectUnauthorized:true,ca}};
  const httpAlchemy=alchemyEndpoint(env,'RM_MARKETPLACE_ALCHEMY_HTTP_URL','https:','DEDICATED_ALCHEMY_HTTP_REQUIRED');
  const wsAlchemy=alchemyEndpoint(env,'RM_MARKETPLACE_ALCHEMY_WS_URL','wss:','DEDICATED_ALCHEMY_WS_REQUIRED');
  // Both transports must come from the same dedicated Alchemy app key. Never
  // consult legacy or frontend endpoint variables as a fallback.
  if(httpAlchemy.pathname!==wsAlchemy.pathname)invalid();
  const result={enabled:true,manifest,stream,connection,login,httpRpcUrl:httpAlchemy.href,wsRpcUrl:wsAlchemy.href,
    // This interval is only for advisory-lock standby; chain progress is event
    // driven with a low-frequency canonical HTTP fallback.
    pollMs:number(env,'RM_POLL_INTERVAL_MS',60000,15000,300000),
    // Alchemy documents a 10-block eth_getLogs range for Robinhood Mainnet on
    // Free. Larger batches remain explicit and must match the dedicated plan.
    batchSize:number(env,'RM_BATCH_SIZE',10,1,100),
    confirmations:number(env,'RM_CONFIRMATIONS',12,1,1000),
    wsFallbackMs:number(env,'RM_WS_FALLBACK_INTERVAL_MS',300000,30000,1800000),
    wsReconnectMinMs:number(env,'RM_WS_RECONNECT_MIN_MS',1000,250,30000),
    wsReconnectMaxMs:number(env,'RM_WS_RECONNECT_MAX_MS',30000,1000,300000),
    catchupBatchDelayMs:number(env,'RM_CATCHUP_BATCH_DELAY_MS',1000,0,60000),
    alertLagBlocks:number(env,'RM_ALERT_LAG_BLOCKS',5000,1,1000000),
    alertStallMs:number(env,'RM_ALERT_STALL_MS',300000,30000,3600000),
    retryMinMs:number(env,'RM_RETRY_MIN_MS',30000,15000,300000),
    retryMaxMs:number(env,'RM_RETRY_MAX_MS',300000,30000,900000),
    shutdownMs:number(env,'RM_SHUTDOWN_TIMEOUT_MS',90000,30000,105000),
  };
  if(result.retryMaxMs<result.retryMinMs||result.wsReconnectMaxMs<result.wsReconnectMinMs)invalid();
  return result;
}
