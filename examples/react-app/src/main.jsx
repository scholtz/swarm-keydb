import React from 'react';
import { createRoot } from 'react-dom/client';
import { SwarmKeyDbProvider, useSwarmDelete, useSwarmKeys, useSwarmPut, useSwarmValue } from '../../../swarm-keydb-react/index.js';

function App() {
  const { value, loading, error } = useSwarmValue('profile:name');
  const { put, isLoading: putLoading } = useSwarmPut('profile:name');
  const { remove, isLoading: deleteLoading } = useSwarmDelete('profile:name');
  const { keys, loading: keysLoading } = useSwarmKeys('profile:');

  return (
    <main style={{ fontFamily: 'sans-serif', maxWidth: 720, margin: '0 auto', padding: 24 }}>
      <h1>SwarmKeyDb React hooks example</h1>
      <p>Current value: {loading ? 'loading...' : String(value ?? 'missing')}</p>
      {error ? <p style={{ color: 'crimson' }}>Error: {String(error.message ?? error)}</p> : null}
      <button disabled={putLoading} onClick={() => put('Ada')}>
        Save profile:name=Ada
      </button>{' '}
      <button disabled={deleteLoading} onClick={() => remove()}>
        Delete profile:name
      </button>
      <h2>profile:* keys</h2>
      {keysLoading ? <p>Loading keys...</p> : <pre>{JSON.stringify(keys, null, 2)}</pre>}
    </main>
  );
}

createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <SwarmKeyDbProvider>
      <App />
    </SwarmKeyDbProvider>
  </React.StrictMode>
);
