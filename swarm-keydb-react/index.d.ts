import type { ReactNode } from 'react';
import type { SwarmKeyDb, SwarmKeyDbOptions } from 'swarm-keydb-js';

export type SwarmDeserialize<T> = (value: string | null) => T | null;
export type SwarmSerialize<T> = (value: T) => string;

export type SwarmKeyDbProviderProps = {
  children: ReactNode;
  client?: SwarmKeyDb;
  clientOptions?: SwarmKeyDbOptions;
  autoConnect?: boolean;
};

export type UseSwarmValueOptions<T> = {
  deserialize?: SwarmDeserialize<T>;
  suspense?: boolean;
  throwErrors?: boolean;
};

export type UseSwarmPutOptions<T> = {
  serialize?: SwarmSerialize<T>;
};

export type UseSwarmQueryOptions = {
  suspense?: boolean;
  throwErrors?: boolean;
};

export type UseSwarmValueResult<T> = {
  value: T | null;
  loading: boolean;
  error: unknown;
  refresh: () => Promise<T | null>;
};

export type UseSwarmPutResult<T> = {
  put: (value: T) => Promise<T>;
  isLoading: boolean;
  error: unknown;
};

export type UseSwarmDeleteResult = {
  remove: () => Promise<boolean>;
  isLoading: boolean;
  error: unknown;
};

export type UseSwarmKeysResult = {
  keys: string[];
  loading: boolean;
  error: unknown;
  refresh: () => Promise<string[]>;
};

export declare function SwarmKeyDbProvider(props: SwarmKeyDbProviderProps): ReactNode;

export declare function useSwarmKeyDb(): {
  client: SwarmKeyDb;
  isConnected: boolean;
  connectError: unknown;
};

export declare function useSwarmValue<T = string>(
  key: string,
  options?: UseSwarmValueOptions<T>
): UseSwarmValueResult<T>;

export declare function useSwarmPut<T = string>(
  key: string,
  options?: UseSwarmPutOptions<T>
): UseSwarmPutResult<T>;

export declare function useSwarmDelete(key: string): UseSwarmDeleteResult;

export declare function useSwarmKeys(prefix?: string, options?: UseSwarmQueryOptions): UseSwarmKeysResult;
