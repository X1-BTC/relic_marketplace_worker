// Static checks only: no worker imports, clients, environment reads or SQL.
import {readFileSync,readdirSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const root=fileURLToPath(new URL('../',import.meta.url));
const read=path=>readFileSync(new URL('../'+path,import.meta.url),'utf8');
const pkg=JSON.parse(read('package.json'));
const lock=JSON.parse(read('package-lock.json'));
const build=JSON.parse(read('railpack.json'));
const start='node --max-old-space-size=192 indexer/worker.mjs';
assert.equal(pkg.scripts['start:marketplace-worker'],start);
assert.equal(build.deploy.startCommand,start);
assert.equal(build.packages.node,'24.22.1');
assert.deepEqual(build.steps.build.commands,['npm run check:marketplace-worker']);
assert.deepEqual(build.steps.install.commands,['npm ci --omit=dev --ignore-scripts --no-audit --no-fund']);
for(const name of ['pg','viem','ethers']) {
  assert.match(pkg.dependencies[name],/^\d+\.\d+\.\d+$/);
  assert.equal(lock.packages[''].dependencies[name],pkg.dependencies[name]);
  assert.equal(lock.packages['node_modules/'+name].version,pkg.dependencies[name]);
}
const files=readdirSync(new URL('../indexer/',import.meta.url)).filter(f=>f.endsWith('.mjs')).map(f=>'indexer/'+f);
for(const file of [...files,'scripts/check-marketplace-worker.mjs'])execFileSync(process.execPath,['--check',file],{cwd:root,stdio:'pipe'});
const workerConfig=read('indexer/worker-config.mjs');
assert.match(workerConfig,/RM_MARKETPLACE_ALCHEMY_HTTP_URL/);
assert.match(workerConfig,/RM_MARKETPLACE_ALCHEMY_WS_URL/);
assert.doesNotMatch(workerConfig,/RM_ALCHEMY_RPC_URL|ALCHEMY_RH_MAINNET_RPC_URL|VITE_RH_RPC_URL/);
assert.match(read('indexer/marketplace-ws.mjs'),/method:'eth_subscribe',params:\['logs',\{address:address\.toLowerCase\(\)\}\]/);
assert.match(read('indexer/worker-loop.mjs'),/Subscribe first, then read from the durable checkpoint/);
assert.match(read('indexer/worker-loop.mjs'),/catchupBatchDelayMs/);
console.log('Worker static checks OK: syntax, dependency pins/lockfile, Node 24.22.1, build/start commands. No network or SQL.');
