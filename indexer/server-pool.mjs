// Dependency injection only: no pg dependency installed, no environment access,
// no secret files read and no connection attempted when this module is imported.
export function createServerWriterPool({Pool,connection}) {
  if (typeof Pool!=='function' || !connection || !connection.host || !connection.database || !connection.user || !connection.password) throw Error('Explicit server database connection required');
  if (connection.ssl?.rejectUnauthorized!==true || !connection.ssl.ca || connection.ssl.key || connection.ssl.cert) throw Error('Verified TLS with explicit CA required');
  // Only a deployment-owned secret manager should construct connection. This
  // object must never cross an API/browser boundary or be serialized to logs.
  const pool=new Pool({host:connection.host,port:connection.port ?? 5432,database:connection.database,user:connection.user,password:connection.password,ssl:{rejectUnauthorized:true,ca:connection.ssl.ca},max:2,connectionTimeoutMillis:10000,options:'-c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000'});
  pool.on('error',()=>{}); // Never let an idle driver diagnostic expose credentials.
  return {
    async connect() {
      let db;
      try {
        db=await pool.connect();
        await db.query('SET ROLE relic_market_writer');
        await db.query('SET search_path=pg_catalog');
        return db;
      } catch { db?.release(true); throw Error('Writer database connection unavailable'); }
    },
    end:()=>pool.end(),
  };
}
