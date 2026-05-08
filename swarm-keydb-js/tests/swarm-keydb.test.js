import test from 'node:test';
import assert from 'node:assert/strict';
import { ConnectionError, KeyNotFoundError, SwarmKeyDb } from '../index.js';

function createMockClient(overrides = {}) {
  return {
    connect: async () => {},
    quit: async () => {},
    disconnect: async () => {},
    get: async (key) => (key === 'missing' ? null : 'value'),
    set: async () => 'OK',
    del: async () => 1,
    keys: async () => ['a', 'b'],
    mGet: async (keys) => keys.map((k) => `v:${k}`),
    mSet: async () => 'OK',
    setEx: async () => 'OK',
    ...overrides
  };
}

test('put/get/delete/list happy path', async () => {
  const mock = createMockClient();
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => mock);

  await db.connect();
  await db.put('k', 'v');
  assert.equal(await db.get('k'), 'value');
  assert.equal(await db.delete('k'), true);
  assert.deepEqual(await db.list('a*'), ['a', 'b']);
  await db.disconnect();
});

test('batchGet and batchPut', async () => {
  let written;
  const mock = createMockClient({ mSet: async (pairs) => { written = pairs; } });
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => mock);

  await db.connect();
  assert.deepEqual(await db.batchGet(['a', 'b']), ['v:a', 'v:b']);
  await db.batchPut([{ key: 'a', value: '1' }, { key: 'b', value: '2' }]);
  assert.deepEqual(written, { a: '1', b: '2' });
});

test('setWithTTL validates ttl', async () => {
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => createMockClient());
  await db.connect();
  await assert.rejects(() => db.setWithTTL('a', 'v', 0), /positive integer/);
  await db.setWithTTL('a', 'v', 2);
});

test('getOrThrow raises KeyNotFoundError', async () => {
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => createMockClient());
  await db.connect();
  await assert.rejects(() => db.getOrThrow('missing'), KeyNotFoundError);
});

test('connection failures are descriptive', async () => {
  const db = new SwarmKeyDb(
    { host: 'localhost', port: 6379 },
    () => createMockClient({ connect: async () => { throw new Error('ECONNREFUSED'); } })
  );

  await assert.rejects(() => db.connect(), ConnectionError);
});
