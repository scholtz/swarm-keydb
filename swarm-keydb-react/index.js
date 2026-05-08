import React, { createContext, useContext, useEffect, useMemo, useRef, useState } from 'react';

const SwarmKeyDbContext = createContext(null);

const defaultSerialize = (value) => (typeof value === 'string' ? value : JSON.stringify(value));
const defaultDeserialize = (value) => {
  if (value === null || value === undefined) {
    return null;
  }

  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
};

function createStore() {
  const keyListeners = new Map();
  const changeListeners = new Set();
  const cache = new Map();

  const subscribeKey = (key, listener) => {
    const listeners = keyListeners.get(key) ?? new Set();
    listeners.add(listener);
    keyListeners.set(key, listeners);
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) {
        keyListeners.delete(key);
      }
    };
  };

  const subscribeChange = (listener) => {
    changeListeners.add(listener);
    return () => changeListeners.delete(listener);
  };

  const publish = (key, value, exists = true) => {
    if (exists) {
      cache.set(key, value);
    } else {
      cache.delete(key);
    }

    keyListeners.get(key)?.forEach((listener) => listener(value, exists));
    changeListeners.forEach((listener) => listener({ key, value, exists }));
  };

  return {
    cache,
    subscribeKey,
    subscribeChange,
    publish,
    getCached: (key) => cache.get(key),
    hasCached: (key) => cache.has(key)
  };
}

function createDeferredClient(clientOptions) {
  let resolvedClient = null;
  let resolvePromise = null;

  const getClient = async () => {
    if (resolvedClient) {
      return resolvedClient;
    }

    if (!resolvePromise) {
      const moduleName = 'swarm-keydb-js';
      resolvePromise = import(/* @vite-ignore */ moduleName).then(({ SwarmKeyDb }) => {
        resolvedClient = new SwarmKeyDb(clientOptions);
        return resolvedClient;
      });
    }

    return resolvePromise;
  };

  return {
    async connect() {
      const client = await getClient();
      await client.connect();
    },
    async disconnect() {
      const client = await getClient();
      await client.disconnect();
    },
    async get(key) {
      const client = await getClient();
      return client.get(key);
    },
    async put(key, value) {
      const client = await getClient();
      return client.put(key, value);
    },
    async delete(key) {
      const client = await getClient();
      return client.delete(key);
    },
    async list(pattern) {
      const client = await getClient();
      return client.list(pattern);
    }
  };
}

export function SwarmKeyDbProvider({
  children,
  client,
  clientOptions = { host: '127.0.0.1', port: 6379 },
  autoConnect = true
}) {
  const storeRef = useRef(createStore());
  const clientRef = useRef(client ?? createDeferredClient(clientOptions));
  const [isConnected, setIsConnected] = useState(false);
  const [connectError, setConnectError] = useState(null);

  useEffect(() => {
    if (!autoConnect) {
      return undefined;
    }

    let isActive = true;
    const connect = async () => {
      try {
        await clientRef.current.connect();
        if (isActive) {
          setIsConnected(true);
          setConnectError(null);
        }
      } catch (error) {
        if (isActive) {
          setConnectError(error);
        }
      }
    };

    connect();

    return () => {
      isActive = false;
      if (isConnected) {
        clientRef.current.disconnect().catch(() => {});
      }
    };
  }, [autoConnect, isConnected]);

  const contextValue = useMemo(() => ({
    client: clientRef.current,
    store: storeRef.current,
    isConnected,
    connectError
  }), [isConnected, connectError]);

  return React.createElement(SwarmKeyDbContext.Provider, { value: contextValue }, children);
}

export function useSwarmKeyDb() {
  const context = useContext(SwarmKeyDbContext);
  if (!context) {
    throw new Error('SwarmKeyDbProvider is required. Wrap your component tree with <SwarmKeyDbProvider>.');
  }

  return context;
}

export function useSwarmValue(key, options = {}) {
  const { client, store } = useSwarmKeyDb();
  const deserialize = options.deserialize ?? defaultDeserialize;
  const suspense = options.suspense === true;
  const throwErrors = options.throwErrors === true;
  const [state, setState] = useState(() => ({
    value: store.hasCached(key) ? store.getCached(key) : null,
    loading: !store.hasCached(key),
    error: null
  }));
  const pendingPromiseRef = useRef(null);

  const refresh = async () => {
    setState((current) => ({ ...current, loading: true, error: null }));
    const pending = client.get(key)
      .then((raw) => {
        const parsed = deserialize(raw);
        store.publish(key, parsed, raw !== null);
        setState({ value: parsed, loading: false, error: null });
        return parsed;
      })
      .catch((error) => {
        setState((current) => ({ ...current, loading: false, error }));
        throw error;
      })
      .finally(() => {
        if (pendingPromiseRef.current === pending) {
          pendingPromiseRef.current = null;
        }
      });

    pendingPromiseRef.current = pending;
    return pending;
  };

  useEffect(() => {
    const unsubscribe = store.subscribeKey(key, (value, exists) => {
      setState({ value: exists ? value : null, loading: false, error: null });
    });

    if (!store.hasCached(key)) {
      refresh().catch(() => {});
    }

    return unsubscribe;
  }, [client, deserialize, key, store]);

  if (throwErrors && state.error) {
    throw state.error;
  }

  if (suspense && state.loading && pendingPromiseRef.current) {
    throw pendingPromiseRef.current;
  }

  return { ...state, refresh };
}

export function useSwarmPut(key, options = {}) {
  const { client, store } = useSwarmKeyDb();
  const serialize = options.serialize ?? defaultSerialize;
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  const put = async (value) => {
    const hadPrevious = store.hasCached(key);
    const previous = hadPrevious ? store.getCached(key) : null;

    setIsLoading(true);
    setError(null);
    store.publish(key, value, true);

    try {
      await client.put(key, serialize(value));
      return value;
    } catch (putError) {
      if (hadPrevious) {
        store.publish(key, previous, true);
      } else {
        store.publish(key, null, false);
      }

      setError(putError);
      throw putError;
    } finally {
      setIsLoading(false);
    }
  };

  return { put, isLoading, error };
}

export function useSwarmDelete(key) {
  const { client, store } = useSwarmKeyDb();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  const remove = async () => {
    setIsLoading(true);
    setError(null);

    try {
      const removed = await client.delete(key);
      if (removed) {
        store.publish(key, null, false);
      }

      return removed;
    } catch (deleteError) {
      setError(deleteError);
      throw deleteError;
    } finally {
      setIsLoading(false);
    }
  };

  return { remove, isLoading, error };
}

export function useSwarmKeys(prefix = '', options = {}) {
  const { client, store } = useSwarmKeyDb();
  const suspense = options.suspense === true;
  const throwErrors = options.throwErrors === true;
  const [state, setState] = useState({ keys: [], loading: true, error: null });
  const pendingPromiseRef = useRef(null);

  const refresh = async () => {
    setState((current) => ({ ...current, loading: true, error: null }));
    const pattern = prefix.length > 0 ? `${prefix}*` : '*';
    const pending = client.list(pattern)
      .then((keys) => {
        const sorted = [...keys].sort();
        setState({ keys: sorted, loading: false, error: null });
        return sorted;
      })
      .catch((error) => {
        setState((current) => ({ ...current, loading: false, error }));
        throw error;
      })
      .finally(() => {
        if (pendingPromiseRef.current === pending) {
          pendingPromiseRef.current = null;
        }
      });

    pendingPromiseRef.current = pending;
    return pending;
  };

  useEffect(() => {
    const updateKeys = ({ key, exists }) => {
      if (prefix.length > 0 && !key.startsWith(prefix)) {
        return;
      }

      setState((current) => {
        const asSet = new Set(current.keys);
        if (exists) {
          asSet.add(key);
        } else {
          asSet.delete(key);
        }

        return { ...current, keys: [...asSet].sort() };
      });
    };

    const unsubscribe = store.subscribeChange(updateKeys);
    refresh().catch(() => {});
    return unsubscribe;
  }, [client, prefix, store]);

  if (throwErrors && state.error) {
    throw state.error;
  }

  if (suspense && state.loading && pendingPromiseRef.current) {
    throw pendingPromiseRef.current;
  }

  return { ...state, refresh };
}
