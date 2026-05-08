import test from 'node:test';
import assert from 'node:assert/strict';
import { ConnectionError, KeyNotFoundError, PrivacyMode, DidAuthMode, SwarmKeyDb } from '../index.js';

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
    sendCommand: async (args) => {
      if (args[0] === 'BACKUP') {
        return 'swarm://backup-ref';
      }
      if (args[0] === 'RESTOREDB') {
        return 2;
      }
      if (args[0] === 'ROTATEKEY') {
        return 'swarm://rotation-ref';
      }
      throw new Error(`unexpected command ${args[0]}`);
    },
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

test('backup restore and rotateKey use management commands', async () => {
  const commands = [];
  const mock = createMockClient({
    sendCommand: async (args) => {
      commands.push(args);
      if (args[0] === 'BACKUP') {
        return 'swarm://backup-ref';
      }
      if (args[0] === 'RESTOREDB') {
        return 2;
      }
      if (args[0] === 'ROTATEKEY') {
        return 'swarm://rotation-ref';
      }

      throw new Error('unexpected command');
    }
  });
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => mock);
  await db.connect();

  assert.equal(await db.backup(), 'swarm://backup-ref');
  assert.equal(await db.restore('swarm://backup-ref', 'old-key'), 2);
  assert.equal(await db.rotateKey('old-key', 'new-key'), 'swarm://rotation-ref');
  assert.deepEqual(commands, [
    ['BACKUP'],
    ['RESTOREDB', 'swarm://backup-ref', 'old-key'],
    ['ROTATEKEY', 'old-key', 'new-key']
  ]);
});

test('privacy mode tokenizes outbound keys while list stays plaintext', async () => {
  let lastSetKey = null;
  let lastGetKey = null;
  const mock = createMockClient({
    set: async (key) => { lastSetKey = key; },
    get: async (key) => {
      lastGetKey = key;
      return 'value';
    }
  });
  const db = new SwarmKeyDb(
    { host: 'localhost', port: 6379, privacyMode: PrivacyMode.ObliviousHashing, privacyKey: '00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff' },
    () => mock
  );
  await db.connect();
  await db.put('secret:key', 'v');
  await db.get('secret:key');
  assert.notEqual(lastSetKey, 'secret:key');
  assert.notEqual(lastGetKey, 'secret:key');
  assert.deepEqual(await db.list('secret:*'), ['secret:key']);
});

test('didAuthMode option is stored on instance', () => {
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379, didMode: 'ethr_did', didRpcUrl: 'http://localhost:8545' }, () => createMockClient());
  assert.equal(db.didMode, 'ethr_did');
});

test('setDid sends AUTHDID command without proof', async () => {
  const commands = [];
  const mock = createMockClient({
    sendCommand: async (args) => { commands.push(args); return 'OK'; }
  });
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => mock);
  await db.connect();
  await db.setDid('did:ethr:0x1111111111111111111111111111111111111111');
  assert.deepEqual(commands, [['AUTHDID', 'did:ethr:0x1111111111111111111111111111111111111111']]);
  assert.equal(db._currentDid, 'did:ethr:0x1111111111111111111111111111111111111111');
});

test('setDid sends AUTHDID command with proof', async () => {
  const commands = [];
  const mock = createMockClient({
    sendCommand: async (args) => { commands.push(args); return 'OK'; }
  });
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => mock);
  await db.connect();
  await db.setDid('did:ethr:0x1234', 'msg', '0xsig');
  assert.deepEqual(commands, [['AUTHDID', 'did:ethr:0x1234', 'msg', '0xsig']]);
});

test('clearDid resets DID context', async () => {
  const db = new SwarmKeyDb({ host: 'localhost', port: 6379 }, () => createMockClient());
  await db.connect();
  db._currentDid = 'did:ethr:0x1111111111111111111111111111111111111111';
  db.clearDid();
  assert.equal(db._currentDid, null);
});
