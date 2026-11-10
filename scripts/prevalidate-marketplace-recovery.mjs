import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';

const local=[
  ['targeted recovery tests',['node','scripts/test-targeted-marketplace-recovery.mjs']],
  ['sparse journal tests',['node','scripts/test-sparse-journal.mjs']],
  ['manifest migration tests',['node','scripts/test-marketplace-manifest-migration.mjs']],
  ['migration script syntax',['node','--check','scripts/migrate-marketplace-manifest.mjs']],
  ['replay script syntax',['node','--check','scripts/replay-marketplace-history.mjs']],
  ['audit script syntax',['node','--check','scripts/audit-marketplace-history.mjs']],
  ['build',['npm','run','build']],
  ['diff check',['git','diff','--check']],
];
const online=[
  ['manifest read-only preflight',['node','scripts/migrate-marketplace-manifest.mjs','--dry-run']],
  ['targeted receipt and Supabase audit',['node','scripts/audit-marketplace-history.mjs','--targeted']],
  ['historical replay dry-run',['node','scripts/replay-marketplace-history.mjs','--dry-run']],
];
function run(label,parts){
  const [command,...args]=parts;process.stdout.write(`PREVALIDATE ${label}\n`);
  const child=spawnSync(command,args,{stdio:'inherit',env:process.env});
  if(child.error||child.status!==0)throw Error(`PREVALIDATION_FAILED:${label}`);
}
function main(){
  for(const [label,parts] of local)run(label,parts);
  if(process.argv.includes('--online-read-only'))for(const [label,parts] of online)run(label,parts);
  process.stdout.write(JSON.stringify({event:'marketplace_recovery_prevalidation_passed',onlineReadOnly:process.argv.includes('--online-read-only'),productionWrites:false})+'\n');
}
if(import.meta.url===pathToFileURL(process.argv[1]??'').href)try{main();}catch(error){process.stdout.write(JSON.stringify({event:'marketplace_recovery_prevalidation_failed',code:error.message})+'\n');process.exitCode=1;}
