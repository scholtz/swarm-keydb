export type PrivacyMode = 'none' | 'oblivious_hashing' | 'full_psi';

export type SwarmKeyDbOptions = {
  host: string;
  port: number;
  password?: string;
  tls?: boolean;
  privacyMode?: PrivacyMode;
  privacyKey?: string;
};

export type BatchEntry = { key: string; value: string };

export declare class SwarmKeyDbError extends Error {
  cause?: unknown;
}

export declare class ConnectionError extends SwarmKeyDbError {}
export declare class KeyNotFoundError extends SwarmKeyDbError {}
export declare const PrivacyMode: {
  readonly None: 'none';
  readonly ObliviousHashing: 'oblivious_hashing';
  readonly FullPSI: 'full_psi';
};

export declare class SwarmKeyDb {
  constructor(options: SwarmKeyDbOptions, clientFactory?: (url: string) => any);
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  get(key: string): Promise<string | null>;
  getOrThrow(key: string): Promise<string>;
  put(key: string, value: string): Promise<void>;
  delete(key: string): Promise<boolean>;
  list(pattern?: string): Promise<string[]>;
  batchGet(keys: string[]): Promise<Array<string | null>>;
  batchPut(entries: Record<string, string> | BatchEntry[]): Promise<void>;
  setWithTTL(key: string, value: string, ttlSeconds: number): Promise<void>;
  backup(): Promise<string>;
  restore(ref: string, key?: string): Promise<number>;
  rotateKey(oldKey: string, newKey: string): Promise<string>;
}
