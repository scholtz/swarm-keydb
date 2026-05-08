import test from 'node:test';
import assert from 'node:assert/strict';
import React from 'react';
import TestRenderer, { act } from 'react-test-renderer';
import { SwarmKeyDbProvider, useSwarmDelete, useSwarmKeys, useSwarmPut, useSwarmValue } from '../index.js';

function createMockClient() {
  const store = new Map();
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

function renderWithProvider(client, hook) {
  const state = { current: null };

  function Harness() {
    state.current = hook();
    return null;
  }

  act(() => {
    TestRenderer.create(
      React.createElement(
        SwarmKeyDbProvider,
        { client },
        React.createElement(Harness)
      )
    );
  });

  return state;
}

test('useSwarmValue loads and updates values', async () => {
  const client = createMockClient();
  client.store.set('profile:name', 'Ada');

  const state = renderWithProvider(client, () => useSwarmValue('profile:name'));

  await act(async () => {
    await state.current.refresh();
  });

  assert.equal(state.current.loading, false);
  assert.equal(state.current.value, 'Ada');
});

test('useSwarmPut performs optimistic update and rollback on failure', async () => {
  const client = createMockClient();
  client.store.set('profile:name', 'Ada');
  const putError = new Error('put failed');
  let failNextPut = true;
  client.put = async () => {
    if (failNextPut) {
      failNextPut = false;
      throw putError;
    }
  };

  const state = renderWithProvider(client, () => {
    const value = useSwarmValue('profile:name');
    const put = useSwarmPut('profile:name');
    return { value, put };
  });

  await act(async () => {
    await state.current.value.refresh();
  });

  await assert.rejects(async () => {
    await act(async () => {
      await state.current.put.put('Grace');
    });
  }, /put failed/);

  assert.equal(state.current.value.value, 'Ada');
});

test('useSwarmDelete removes key and useSwarmKeys tracks updates', async () => {
  const client = createMockClient();
  client.store.set('profile:name', 'Ada');
  client.store.set('profile:theme', 'dark');

  const state = renderWithProvider(client, () => {
    const keys = useSwarmKeys('profile:');
    const remove = useSwarmDelete('profile:name');
    return { keys, remove };
  });

  await act(async () => {
    await state.current.keys.refresh();
  });

  assert.deepEqual(state.current.keys.keys, ['profile:name', 'profile:theme']);

  await act(async () => {
    await state.current.remove.remove();
  });

  assert.deepEqual(state.current.keys.keys, ['profile:theme']);
});
