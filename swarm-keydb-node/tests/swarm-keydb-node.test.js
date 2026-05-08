import test from 'node:test';
import assert from 'node:assert/strict';
import { SwarmKeyDbService, createExpressCacheMiddleware, createExpressSwarmKeyDbMiddleware } from '../index.js';

function createMockClient(initialEntries = {}) {
  const store = new Map(Object.entries(initialEntries));
  return {
    store,
    connect: async () => {},
    disconnect: async () => {},
    get: async (key) => (store.has(key) ? store.get(key) : null),
    put: async (key, value) => {
      store.set(key, value);
    },
    delete: async (key) => store.delete(key),
    list: async (pattern) => {
      const prefix = pattern === '*' ? '' : pattern.slice(0, -1);
      return [...store.keys()].filter((key) => key.startsWith(prefix));
    }
  };
}

test('service uses retry policy and round-robin pool', async () => {
  let attempts = 0;
  const first = createMockClient({ 'k:1': 'a' });
  first.get = async () => {
    attempts += 1;
    if (attempts < 2) {
      throw new Error('transient');
    }

    return 'a';
  };
  const second = createMockClient({ 'k:1': 'a' });

  const clients = [first, second];
  const service = new SwarmKeyDbService({
    poolSize: 2,
    maxRetries: 2,
    retryDelayMs: 0,
    clientFactory: () => clients.shift()
  });

  const value = await service.get('k:1');
  assert.equal(value, 'a');

  await service.dispose();
});

test('service scan streams keys in batches', async () => {
  const service = new SwarmKeyDbService({
    poolSize: 1,
    clientFactory: () => createMockClient({
      'profile:1': 'a',
      'profile:2': 'b',
      'profile:3': 'c'
    })
  });

  const keys = [];
  for await (const key of service.scan('profile:', 2)) {
    keys.push(key);
  }

  assert.deepEqual(keys.sort(), ['profile:1', 'profile:2', 'profile:3']);
  await service.dispose();
});

test('express middleware attaches session helper', async () => {
  const service = new SwarmKeyDbService({
    poolSize: 1,
    clientFactory: () => createMockClient({ 'session:abc': JSON.stringify({ id: 'abc' }) })
  });

  const middleware = createExpressSwarmKeyDbMiddleware(service, {
    sessionKeyResolver: () => 'session:abc'
  });

  const req = { headers: {} };
  const res = {};

  let called = false;
  await middleware(req, res, () => {
    called = true;
  });

  assert.equal(called, true);
  assert.equal(req.swarmSession.id, 'abc');
  await req.saveSwarmSession({ id: 'abc', role: 'admin' });

  const saved = await service.get('session:abc');
  assert.equal(saved.role, 'admin');
  await service.dispose();
});

test('express cache middleware returns cached value then stores misses', async () => {
  const client = createMockClient({ 'cache:GET:/cached': JSON.stringify({ ok: true }) });
  const service = new SwarmKeyDbService({ poolSize: 1, clientFactory: () => client });
  const middleware = createExpressCacheMiddleware(service, {
    keyResolver: (req) => `cache:${req.method}:${req.url}`
  });

  const hitReq = { method: 'GET', url: '/cached' };
  const hitRes = {
    headers: {},
    setHeader(name, value) {
      this.headers[name] = value;
    },
    json(payload) {
      this.payload = payload;
    }
  };

  let hitNextCalled = false;
  await middleware(hitReq, hitRes, () => {
    hitNextCalled = true;
  });

  assert.equal(hitRes.headers['x-swarm-cache'], 'HIT');
  assert.equal(hitNextCalled, false);

  const missReq = { method: 'GET', url: '/miss' };
  const missRes = {
    headers: {},
    setHeader(name, value) {
      this.headers[name] = value;
    },
    json(payload) {
      this.payload = payload;
      return payload;
    }
  };

  let missNextCalled = false;
  await middleware(missReq, missRes, () => {
    missNextCalled = true;
    missRes.json({ ok: true });
  });

  assert.equal(missNextCalled, true);
  assert.equal(missRes.headers['x-swarm-cache'], 'MISS');
  await service.dispose();
});
