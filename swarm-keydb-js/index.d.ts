export type PrivacyMode = 'none' | 'oblivious_hashing' | 'full_psi';
export type DidAuthMode = 'none' | 'ethr_did';
export type OfflineMode = 'never' | 'auto' | 'always';

export type SwarmKeyDbOptions = {
  host: string;
  port: number;
  password?: string;
  tls?: boolean;
  privacyMode?: PrivacyMode;
  privacyKey?: string;
  /** DID authentication mode. Defaults to 'none'. */
  didMode?: DidAuthMode;
  /** Ethereum JSON-RPC URL for on-chain DID resolution. */
  didRpcUrl?: string;
  /** DID method string, e.g. "ethr". */
  didMethod?: string;
  /** Offline mode hint for parity with the C# store. */
  offlineMode?: OfflineMode;
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
export declare const DidAuthMode: {
  readonly None: 'none';
  readonly EthrDid: 'ethr_did';
};
export declare const OfflineMode: {
  readonly Never: 'never';
  readonly Auto: 'auto';
  readonly Always: 'always';
};

export declare class SwarmKeyDb {
  constructor(options: SwarmKeyDbOptions, clientFactory?: (url: string) => any);
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  /**
   * Registers a DID for this connection. When proofMessage and proofSignature are provided,
   * the server verifies the proof immediately.
   */
  setDid(did: string, proofMessage?: string, proofSignature?: string): Promise<void>;
  /** Clears the current DID context from this connection. */
  clearDid(): void;
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
