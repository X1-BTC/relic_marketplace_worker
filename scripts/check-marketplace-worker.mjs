
// Static checks only: no worker imports, clients, environment reads or SQL.
import { readFileSync, readdirSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const read = path =>
  readFileSync(new URL('../' + path, import.meta.url), 'utf8');

const pkg = JSON.parse(read('package.json'));
const lock = JSON.parse(read('package-lock.json'));

const start = 'node --max-old-space-size=192 indexer/worker.mjs';
const requiredNode = '24.22.1';
const requiredEngine = '>=24.22.1 <25';

// Node.js version and configuration checks.
assert.equal(pkg.engines.node, requiredEngine);
assert.equal(read('.node-version').trim(), requiredNode);

const actualNode = process.versions.node.split('.').map(Number);
assert.equal(actualNode.length, 3);
assert.ok(
  actualNode[0] === 24 &&
  (actualNode[1] > 22 ||
    (actualNode[1] === 22 && actualNode[2] >= 1)),
  `Node.js ${process.versions.node} is incompatible; expected >=24.22.1 <25`
);

// Package scripts and lockfile checks.
assert.equal(pkg.scripts.start, start);
assert.equal(pkg.scripts['start:marketplace-worker'], start);
assert.equal(pkg.scripts.build, 'npm run check:marketplace-worker');
assert.equal(
  pkg.scripts['check:marketplace-worker'],
  'node scripts/check-marketplace-worker.mjs'
);
assert.equal(lock.packages[''].engines.node, requiredEngine);

for (const name of ['pg', 'viem', 'ethers']) {
  assert.match(pkg.dependencies[name], /^\d+\.\d+\.\d+$/);
  assert.equal(
    lock.packages[''].dependencies[name],
    pkg.dependencies[name]
  );
  assert.equal(
    lock.packages['node_modules/' + name].version,
    pkg.dependencies[name]
  );
}

// Syntax checks.
const files = readdirSync(new URL('../indexer/', import.meta.url))
  .filter(f => f.endsWith('.mjs'))
  .map(f => 'indexer/' + f);

for (const file of [...files, 'scripts/check-marketplace-worker.mjs']) {
  execFileSync(process.execPath, ['--check', file], {
    cwd: root,
    stdio: 'pipe'
  });
}

// Dedicated Alchemy endpoint checks.
const workerConfig = read('indexer/worker-config.mjs');

assert.match(workerConfig, /RM_MARKETPLACE_ALCHEMY_HTTP_URL/);
assert.match(workerConfig, /RM_MARKETPLACE_ALCHEMY_WS_URL/);
assert.doesNotMatch(
  workerConfig,
  /RM_ALCHEMY_RPC_URL|ALCHEMY_RH_MAINNET_RPC_URL|VITE_RH_RPC_URL/
);

// WebSocket subscription and catchup checks.
assert.match(
  read('indexer/marketplace-ws.mjs'),
  /method:'eth_subscribe',params:\['logs',\{address:address\.toLowerCase\(\)\}\]/
);

assert.match(
  read('indexer/worker-loop.mjs'),
  /Subscribe first, then read from the durable checkpoint/
);

assert.match(
  read('indexer/worker-loop.mjs'),
  /catchupBatchDelayMs/
);

console.log(
  'Worker static checks OK: syntax, dependency pins/lockfile, Node >=24.22.1 <25, build/start commands. No network or SQL.'
);
