import React from 'react';
import { SwarmKeyDbProvider, useSwarmDelete, useSwarmKeys, useSwarmPut, useSwarmValue } from '../index.js';

function createStoryClient() {
  const store = new Map([['profile:name', 'Ada']]);
  return {
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

function HookDemo() {
  const { value, loading } = useSwarmValue('profile:name');
  const { put } = useSwarmPut('profile:name');
  const { remove } = useSwarmDelete('profile:name');
  const { keys } = useSwarmKeys('profile:');

  if (loading) {
    return React.createElement('p', null, 'Loading...');
  }

  return React.createElement(
    'div',
    null,
    React.createElement('p', null, `profile:name = ${String(value ?? 'missing')}`),
    React.createElement('button', { onClick: () => put('Grace') }, 'Set Grace'),
    React.createElement('button', { onClick: () => remove() }, 'Delete key'),
    React.createElement('pre', null, JSON.stringify(keys, null, 2))
  );
}

export default {
  title: 'SwarmKeyDb/Hooks',
  component: HookDemo
};

export const Basic = () =>
  React.createElement(
    SwarmKeyDbProvider,
    { client: createStoryClient(), autoConnect: false },
    React.createElement(HookDemo)
  );
