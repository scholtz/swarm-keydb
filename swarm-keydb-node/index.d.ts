import type { SwarmKeyDb, SwarmKeyDbOptions } from 'swarm-keydb-js';

export type Deserialize<T> = (raw: string | null) => T | null;
export type Serialize<T> = (value: T) => string;

export type SwarmKeyDbServiceOptions = {
  clientFactory?: (options: SwarmKeyDbOptions) => SwarmKeyDb | Promise<SwarmKeyDb>;
  clientOptions?: SwarmKeyDbOptions;
  poolSize?: number;
  maxRetries?: number;
  retryDelayMs?: number;
  shouldRetry?: (error: unknown, attemptNumber: number) => boolean;
};

export declare class SwarmKeyDbService {
  constructor(options?: SwarmKeyDbServiceOptions);
  initialize(): Promise<void>;
  dispose(): Promise<void>;
  get<T = string>(key: string, options?: { parse?: Deserialize<T> }): Promise<T | null>;
  put<T = string>(key: string, value: T, options?: { serialize?: Serialize<T> }): Promise<T>;
  delete(key: string): Promise<boolean>;
  list(prefix?: string): Promise<string[]>;
  scan(prefix?: string, batchSize?: number): AsyncIterable<string>;
}

export declare function createExpressSwarmKeyDbMiddleware(
  service: SwarmKeyDbService,
  options?: {
    sessionKeyResolver?: (req: any) => string | undefined;
  }
): (req: any, res: any, next: (error?: unknown) => void) => Promise<void>;

export declare function createExpressCacheMiddleware(
  service: SwarmKeyDbService,
  options?: {
    keyResolver?: (req: any) => string;
  }
): (req: any, res: any, next: (error?: unknown) => void) => Promise<void>;

export declare function createFastifySwarmKeyDbPlugin(
  service: SwarmKeyDbService,
  options?: {
    sessionKeyResolver?: (request: any) => string | undefined;
  }
): (fastify: any) => Promise<void>;
