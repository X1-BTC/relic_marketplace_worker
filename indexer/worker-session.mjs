import {WorkerFault} from './worker-config.mjs';

// The advisory lock AND the writer use this one physical pg client. Keeping a
// lease on a separate connection would leave an in-flight writer unfenced when
// that lease connection is lost. A lost session here cannot continue writing.
export async function acquireWorkerSession(pool,{login,database,signal}) {
  const client=await pool.connect();
  let closed=false;let checkedOut=false;
  const controller=new AbortController();
  const close=()=>{
    if(closed)return;closed=true;controller.abort();
    client.release(true);
  };
  client.on('error',close);client.on('end',close);
  try {
    if(signal.aborted){close();return null;}
    const socket=client.connection?.stream;
    if(!socket?.encrypted||socket.authorized!==true)throw new WorkerFault('DATABASE_TLS_UNVERIFIED',{fatal:true});
    const {rows}=await client.query("SELECT current_user AS role,session_user AS login,current_database() AS database,current_setting('server_version_num')::integer AS version,rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user");
    const row=rows[0];
    if(!row||row.role!=='relic_market_writer'||row.login!==login||row.database!==database||row.version<150000||row.rolsuper||row.rolbypassrls)throw new WorkerFault('DATABASE_IDENTITY_INVALID',{fatal:true});
    const owners=await client.query("SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE (n.nspname='relic_market_indexer' OR (n.nspname='public' AND c.relname LIKE 'relic_market_%')) AND pg_get_userbyid(c.relowner)=current_user) AS owns_tables");
    if(owners.rows[0]?.owns_tables)throw new WorkerFault('DATABASE_OWNER_ROLE',{fatal:true});
    const lock=await client.query("SELECT pg_try_advisory_lock(4663,hashtext('relic_market_indexer:marketplace-v3')) AS acquired");
    if(lock.rows[0]?.acquired!==true){close();return null;}
    return {
      signal:controller.signal,version:row.version,close,
      pool:{async connect(){
        if(closed||checkedOut||signal.aborted)throw new WorkerFault('DATABASE_SESSION_UNAVAILABLE');
        checkedOut=true;let failed=false;
        return {
          async query(sql,args){
            if(closed){failed=true;throw new WorkerFault('DATABASE_SESSION_LOST');}
            try{return await client.query(sql,args);}catch{failed=true;throw new WorkerFault('DATABASE_QUERY_FAILED');}
          },
          // The legacy writer calls release(true) even after success. The leased
          // facade retains successful sessions; ANY query error destroys the real
          // client, so uncertain COMMITs are resolved on a fresh checkpoint read.
          release(){checkedOut=false;if(failed)close();},
        };
      }},
    };
  }catch(error){close();throw error instanceof WorkerFault?error:new WorkerFault('DATABASE_CONNECT_FAILED');}
}
