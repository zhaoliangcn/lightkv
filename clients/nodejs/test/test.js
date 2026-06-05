'use strict';

const assert = require('assert');
const LightKVClient = require('../src/client');

async function runTests() {
  const client = new LightKVClient({
    host: '127.0.0.1',
    port: 16379,
  });

  console.log('[Test] Connecting to server...');
  await client.connect();
  assert(client.isConnected(), 'Should be connected');
  console.log('[Test] Connected');

  // Authenticate
  console.log('[Test] AUTH...');
  const authResult = await client.auth('benchpass123');
  assert(authResult === true, 'AUTH should succeed');
  console.log('[Test] Authenticated');

  // Test 1: Ping
  console.log('[Test] PING...');
  const pong = await client.ping();
  assert(pong === true, 'PING should return true');
  console.log('[Test] PONG received');

  // Test 2: Set/Get
  console.log('[Test] SET/GET...');
  const setResult = await client.set('hello', 'world');
  assert(setResult === true, 'SET should return true');
  const val = await client.get('hello');
  assert(val === 'world', `GET should return "world", got "${val}"`);
  console.log('[Test] GET hello = ' + val);

  // Test 3: Get non-existent key
  console.log('[Test] GET non-existent...');
  const nil = await client.get('nonexistent');
  assert(nil === null, 'GET non-existent should return null');
  console.log('[Test] GET nonexistent = null');

  // Test 4: Delete
  console.log('[Test] DELETE...');
  const delResult = await client.delete('hello');
  assert(delResult === true, 'DELETE should return true');
  const nil2 = await client.get('hello');
  assert(nil2 === null, 'GET after DELETE should return null');
  console.log('[Test] DELETE hello succeeded');

  // Test 5: Set multiple keys
  console.log('[Test] SET multiple...');
  await client.set('a', '1');
  await client.set('b', '2');
  await client.set('c', '3');
  await client.set('d', '4');
  console.log('[Test] Set 4 keys');

  // Test 6: DeleteRange
  console.log('[Test] DELRANGE...');
  const drResult = await client.deleteRange('a', 'c');
  assert(drResult === true, 'DELRANGE should return true');
  const b = await client.get('b');
  assert(b === null, 'b should be deleted');
  const d = await client.get('d');
  assert(d === '4', 'd should still exist');
  console.log('[Test] DELRANGE a-c succeeded, d still exists');

  // Test 7: Stats
  console.log('[Test] STATS...');
  const stats = await client.stats();
  assert(typeof stats === 'object', 'STATS should return an object');
  assert(Object.keys(stats).length > 0, 'STATS should not be empty');
  for (const [k, v] of Object.entries(stats)) {
    console.log('  ' + k + ' = ' + v);
  }

  // Test 8: Quit
  console.log('[Test] QUIT...');
  await client.quit();
  assert(!client.isConnected(), 'Should be disconnected after quit');
  console.log('[Test] Disconnected');

  console.log('\n[All tests passed!]');
}

runTests().catch((err) => {
  console.error('Test failed:', err.message);
  process.exit(1);
});
