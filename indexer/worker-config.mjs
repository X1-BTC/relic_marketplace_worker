import {validateManifest} from './marketplace.mjs';

export const PROJECT_REF='colraxespxkbisfaazeo';
export class WorkerFault extends Error {
  constructor(code,{fatal=false}={}) {super(code);this.code=code;this.fatal=fatal;}
}
const invalid=(reason='unknown')=>{
  console.error(JSON.stringify({event:'config_invalid',reason}));
  throw new WorkerFault('CONFIG_INVALID',{fatal:true});
};
const required=(env,key)=>typeof env[key]==='string'&&env[key].trim()?env[key]:invalid(`missing_or_empty:${key}`);
function number(env,key,fallback,min,max) {
  const raw=env[key] ?? String(fallback);
  if(!/^[0-9]+$/.test(raw))invalid(`bad_number:${key}`);
  const value=Number(raw);if(!Number.isSafeInteger(value)||value<min||value>max)invalid(`out_of_range:${key}`);return value;
}
function alchemyEndpoint(env,key,protocol,missingCode) {
  const raw=env[key];
  if(typeof raw!=='string'||!raw.trim())throw new WorkerFault(missingCode,{fatal:true});
  let parsed;
  try{parsed=new URL(raw);}catch{invalid(`bad_url:${key}`);}
  if(parsed.protocol!==protocol)invalid(`bad_protocol:${key}`);
  if(parsed.hostname!=='robinhood-mainnet.g.alchemy.com')invalid(`bad_hostname:${key}`);
  if(parsed.username||parsed.password||parsed.search||parsed.hash)invalid(`extra_url_parts:${key}`);
  if(!/^\/v2\/[^/]+$/.test(parsed.pathname))invalid(`bad_pathname:${key}`);
  return parsed;
}
export function readWorkerConfig(env) {
  if(env.RM_WORKER_ENABLED!=='true')return {enabled:false};
  if(Object.keys(env).some(k=>k.startsWith('PG') && env[k]))invalid('pg_vars_present');
  if(env.DATABASE_URL || env.RM_DB_CONNECTION_URL)invalid('connection_url_present');
  let manifest;
  try {manifest=validateManifest(JSON.parse(required(env,'RM_MARKETPLACE_MANIFEST')));}catch{invalid('manifest_invalid');}
  if(!Array.isArray(manifest.feeRecipients))invalid('fee_recipients_not_array');
  const stream=required(env,'RM_INDEXER_STREAM');if(!/^[a-z0-9_-]{1,80}$/.test(stream))invalid('bad_stream');
  const mode=required(env,'RM_DB_CONNECTION_MODE');if(!['direct','session'].includes(mode))invalid('bad_connection_mode');
  const host=required(env,'RM_DB_HOST');const user=required(env,'RM_DB_USER');
  const suffix='.'+PROJECT_REF;
  const login=mode==='session'?(user.endsWith(suffix)?user.slice(0,-suffix.length):invalid('user_missing_suffix')):user;
  if(!/^[a-z_][a-z0-9_]{0,62}$/.test(login)||['postgres','service_role','anon','authenticated','authenticator','supabase_admin','relic_market_writer'].includes(login))invalid('bad_login');
  if(mode==='direct' && host!==`db.${PROJECT_REF}.supabase.co`)invalid('bad_direct_host');
  if(mode==='session' && !/^aws-[a-z0-9-]+\.pooler\.supabase\.com$/.test(host))invalid('bad_session_host');
  const port=number(env,'RM_DB_PORT',5432,5432,5432);
  const ca=required(env,'RM_DB_CA_CERT');
  if(!ca.includes('-----BEGIN CERTIFICATE-----')||!ca.includes('-----END CERTIFICATE-----')||ca.includes('PRIVATE KEY'))invalid('bad_ca_cert');
  const database=required(env,'RM_DB_NAME');if(!/^[a-zA-Z0-9_]+$/.test(database))invalid('bad_db_name');
  const connection={host,port,database,user,password:required(env,'RM_DB_PASSWORD'),ssl:{rejectUnauthorized:true,ca}};
  const httpAlchemy=alchemyEndpoint(env,'RM_MARKETPLACE_ALCHEMY_HTTP_URL','https:','DEDICATED_ALCHEMY_HTTP_REQUIRED');
  const wsAlchemy=alchemyEndpoint(env,'RM_MARKETPLACE_ALCHEMY_WS_URL','wss:','DEDICATED_ALCHEMY_WS_REQUIRED');
  if(httpAlchemy.pathname!==wsAlchemy.pathname)invalid('alchemy_keys_mismatch');
  const result={enabled:true,manifest,stream,connection,login,httpRpcUrl:httpAlchemy.href,wsRpcUrl:wsAlchemy.href,
    pollMs:number(env,'RM_POLL_INTERVAL_MS',60000,15000,300000),
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
  if(result.retryMaxMs<result.retryMinMs||result.wsReconnectMaxMs<result.wsReconnectMinMs)invalid('retry_or_reconnect_range');
  return result;
}
