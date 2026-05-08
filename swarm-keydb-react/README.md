# swarm-keydb-react

React hooks and context provider for `swarm-keydb-js`.

## Install

```bash
npm install swarm-keydb-react swarm-keydb-js react
```

## Quick start

```tsx
import { SwarmKeyDbProvider, useSwarmDelete, useSwarmKeys, useSwarmPut, useSwarmValue } from 'swarm-keydb-react';

function ProfileName() {
  const { value, loading } = useSwarmValue('profile:name');
  const { put } = useSwarmPut('profile:name');
  const { remove } = useSwarmDelete('profile:name');
  const { keys } = useSwarmKeys('profile:');

  if (loading) {
    return <p>Loading...</p>;
  }

  return (
    <div>
      <p>Name: {String(value ?? 'missing')}</p>
      <button onClick={() => put('Ada')}>Set</button>
      <button onClick={() => remove()}>Delete</button>
      <pre>{JSON.stringify(keys, null, 2)}</pre>
    </div>
  );
}

export function App() {
  return (
    <SwarmKeyDbProvider>
      <ProfileName />
    </SwarmKeyDbProvider>
  );
}
```

## Hooks

- `useSwarmValue(key, options?)`: subscribe to a key with loading/error state.
- `useSwarmPut(key, options?)`: optimistic write with rollback on error.
- `useSwarmDelete(key)`: delete helper with loading/error state.
- `useSwarmKeys(prefix?, options?)`: list keys and react to key updates.

## Storybook

```bash
npm install
npm run storybook
```

## Test

```bash
npm install
npm test
```
