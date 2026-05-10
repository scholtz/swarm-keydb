export type SetOptions = {
  ex?: number;
  px?: number;
  nx?: boolean;
  xx?: boolean;
  keepttl?: boolean;
};

export type ConnectOptions = {
  wsUrl?: string;
  httpUrl?: string;
  password?: string;
  reconnect?: boolean;
  reconnectBaseDelayMs?: number;
  reconnectMaxDelayMs?: number;
  requestTimeoutMs?: number;
  httpFallback?: boolean;
  fetchImpl?: typeof fetch;
  webSocketFactory?: (url: string) => Promise<WebSocketLike> | WebSocketLike;
};

export type SubscriptionHandler = (channel: string, message: string) => void;
export type PatternSubscriptionHandler = (pattern: string, channel: string, message: string) => void;

export type StreamReadOptions = {
  blockMs?: number;
  count?: number;
};

export type ZAddEntry = { score: number; member: string };

interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener?: (type: string, listener: (event: { data?: unknown }) => void) => void;
  removeEventListener?: (type: string, listener: (event: { data?: unknown }) => void) => void;
  on?: (event: string, listener: (data?: unknown) => void) => void;
  off?: (event: string, listener: (data?: unknown) => void) => void;
}

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

const OPEN_STATE = 1;

export class SwarmKeyDbClient {
  private readonly options: Required<Omit<ConnectOptions, 'webSocketFactory' | 'fetchImpl'>> & Pick<ConnectOptions, 'webSocketFactory' | 'fetchImpl'>;
  private socket: WebSocketLike | null = null;
  private connectPromise: Promise<void> | null = null;
  private pending: PendingRequest[] = [];
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private manuallyClosed = false;
  private readonly channelHandlers = new Map<string, SubscriptionHandler>();
  private readonly patternHandlers = new Map<string, PatternSubscriptionHandler>();

  constructor(options: ConnectOptions = {}) {
    this.options = {
      wsUrl: options.wsUrl ?? 'ws://127.0.0.1:8765/',
      httpUrl: options.httpUrl ?? 'http://127.0.0.1:8080',
      password: options.password ?? '',
      reconnect: options.reconnect ?? true,
      reconnectBaseDelayMs: options.reconnectBaseDelayMs ?? 250,
      reconnectMaxDelayMs: options.reconnectMaxDelayMs ?? 5_000,
      requestTimeoutMs: options.requestTimeoutMs ?? 10_000,
      httpFallback: options.httpFallback ?? true,
      webSocketFactory: options.webSocketFactory,
      fetchImpl: options.fetchImpl
    };
  }

  async connect(): Promise<void> {
    if (this.socket?.readyState === OPEN_STATE) {
      return;
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.manuallyClosed = false;
    this.connectPromise = this.openSocket();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  async disconnect(): Promise<void> {
    this.manuallyClosed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (!this.socket) {
      return;
    }

    this.socket.close(1000, 'client disconnect');
    this.socket = null;
  }

  async get(key: string): Promise<string | null> {
    return this.execWithHttpFallback<string | null>('GET', [key], async () => {
      const payload = await this.call('GET', key);
      return payload === null ? null : String(payload);
    });
  }

  async set(key: string, value: string, options?: SetOptions): Promise<'OK'> {
    return this.execWithHttpFallback<'OK'>('SET', [key, value], async () => {
      const args = [key, value, ...this.toSetArgs(options)];
      const payload = await this.call('SET', ...args);
      return String(payload) as 'OK';
    });
  }

  async del(...keys: string[]): Promise<number> { return this.asNumber(await this.call('DEL', ...keys)); }
  async exists(key: string): Promise<boolean> { return Boolean(await this.call('EXISTS', key)); }
  async expire(key: string, ttlSeconds: number): Promise<boolean> { return Boolean(await this.call('EXPIRE', key, String(ttlSeconds))); }
  async ttl(key: string): Promise<number> { return this.asNumber(await this.call('TTL', key)); }
  async keys(pattern = '*'): Promise<string[]> { return this.asStringArray(await this.call('KEYS', pattern)); }
  async mget(...keys: string[]): Promise<Array<string | null>> { return (await this.call('MGET', ...keys)) as Array<string | null>; }

  async mset(entries: Record<string, string> | Array<[string, string]>): Promise<'OK'> {
    const tuples = Array.isArray(entries) ? entries : Object.entries(entries);
    const args = tuples.flatMap(([k, v]) => [k, v]);
    return String(await this.call('MSET', ...args)) as 'OK';
  }

  async incr(key: string): Promise<number> { return this.asNumber(await this.call('INCR', key)); }
  async decr(key: string): Promise<number> { return this.asNumber(await this.call('DECR', key)); }
  async incrby(key: string, amount: number): Promise<number> { return this.asNumber(await this.call('INCRBY', key, String(amount))); }

  async hget(key: string, field: string): Promise<string | null> {
    const payload = await this.call('HGET', key, field);
    return payload === null ? null : String(payload);
  }

  async hset(key: string, field: string, value: string): Promise<number> { return this.asNumber(await this.call('HSET', key, field, value)); }
  async hgetall(key: string): Promise<Record<string, string>> {
    const payload = await this.call('HGETALL', key);
    if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
      return Object.fromEntries(Object.entries(payload as Record<string, unknown>).map(([k, v]) => [k, String(v)]));
    }

    const arr = this.asStringArray(payload);
    const out: Record<string, string> = {};
    for (let i = 0; i < arr.length; i += 2) {
      out[arr[i]] = arr[i + 1] ?? '';
    }
    return out;
  }

  async hdel(key: string, ...fields: string[]): Promise<number> { return this.asNumber(await this.call('HDEL', key, ...fields)); }
  async hkeys(key: string): Promise<string[]> { return this.asStringArray(await this.call('HKEYS', key)); }
  async hvals(key: string): Promise<string[]> { return this.asStringArray(await this.call('HVALS', key)); }

  async lpush(key: string, ...values: string[]): Promise<number> { return this.asNumber(await this.call('LPUSH', key, ...values)); }
  async rpush(key: string, ...values: string[]): Promise<number> { return this.asNumber(await this.call('RPUSH', key, ...values)); }
  async lrange(key: string, start: number, stop: number): Promise<string[]> { return this.asStringArray(await this.call('LRANGE', key, String(start), String(stop))); }
  async llen(key: string): Promise<number> { return this.asNumber(await this.call('LLEN', key)); }
  async lpop(key: string): Promise<string | null> { const payload = await this.call('LPOP', key); return payload === null ? null : String(payload); }
  async rpop(key: string): Promise<string | null> { const payload = await this.call('RPOP', key); return payload === null ? null : String(payload); }

  async sadd(key: string, ...members: string[]): Promise<number> { return this.asNumber(await this.call('SADD', key, ...members)); }
  async smembers(key: string): Promise<string[]> { return this.asStringArray(await this.call('SMEMBERS', key)); }
  async sismember(key: string, member: string): Promise<boolean> { return Boolean(await this.call('SISMEMBER', key, member)); }
  async srem(key: string, ...members: string[]): Promise<number> { return this.asNumber(await this.call('SREM', key, ...members)); }
  async scard(key: string): Promise<number> { return this.asNumber(await this.call('SCARD', key)); }

  async zadd(key: string, entries: ZAddEntry[]): Promise<number> {
    const args = entries.flatMap((entry) => [String(entry.score), entry.member]);
    return this.asNumber(await this.call('ZADD', key, ...args));
  }

  async zrange(key: string, start: number, stop: number, withScores = false): Promise<string[]> {
    const args = [key, String(start), String(stop)];
    if (withScores) {
      args.push('WITHSCORES');
    }
    return this.asStringArray(await this.call('ZRANGE', ...args));
  }

  async zscore(key: string, member: string): Promise<number | null> {
    const payload = await this.call('ZSCORE', key, member);
    if (payload === null) {
      return null;
    }
    return Number(payload);
  }

  async zrank(key: string, member: string): Promise<number | null> {
    const payload = await this.call('ZRANK', key, member);
    if (payload === null) {
      return null;
    }
    return Number(payload);
  }

  async zrem(key: string, ...members: string[]): Promise<number> { return this.asNumber(await this.call('ZREM', key, ...members)); }

  async subscribe(channel: string, handler: SubscriptionHandler): Promise<void> {
    this.channelHandlers.set(channel, handler);
    await this.call('SUBSCRIBE', channel);
  }

  async unsubscribe(channel: string): Promise<void> {
    await this.call('UNSUBSCRIBE', channel);
    this.channelHandlers.delete(channel);
  }

  async psubscribe(pattern: string, handler: PatternSubscriptionHandler): Promise<void> {
    this.patternHandlers.set(pattern, handler);
    await this.call('PSUBSCRIBE', pattern);
  }

  async punsubscribe(pattern: string): Promise<void> {
    await this.call('PUNSUBSCRIBE', pattern);
    this.patternHandlers.delete(pattern);
  }

  async publish(channel: string, message: string): Promise<number> {
    return this.asNumber(await this.call('PUBLISH', channel, message));
  }

  async xadd(stream: string, id: string, fields: Record<string, string>): Promise<string> {
    const args = [stream, id, ...Object.entries(fields).flatMap(([k, v]) => [k, v])];
    return String(await this.call('XADD', ...args));
  }

  async xrange(stream: string, start: string, end: string, count?: number): Promise<unknown[]> {
    const args = [stream, start, end];
    if (typeof count === 'number') {
      args.push('COUNT', String(count));
    }
    const payload = await this.call('XRANGE', ...args);
    return Array.isArray(payload) ? payload : [];
  }

  async xread(streams: Record<string, string>, options?: StreamReadOptions): Promise<unknown[] | null> {
    const streamNames = Object.keys(streams);
    const streamOffsets = streamNames.map((name) => streams[name]);
    const args: string[] = [];
    if (typeof options?.blockMs === 'number') {
      args.push('BLOCK', String(options.blockMs));
    }
    if (typeof options?.count === 'number') {
      args.push('COUNT', String(options.count));
    }

    args.push('STREAMS', ...streamNames, ...streamOffsets);
    const payload = await this.call('XREAD', ...args);
    if (payload === null) {
      return null;
    }
    return Array.isArray(payload) ? payload : [];
  }

  async xlen(stream: string): Promise<number> {
    return this.asNumber(await this.call('XLEN', stream));
  }

  async raw(command: string, ...args: string[]): Promise<unknown> {
    return this.call(command, ...args);
  }

  private async execWithHttpFallback<T>(command: 'GET' | 'SET', args: string[], fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (error) {
      if (!this.options.httpFallback) {
        throw error;
      }

      return this.callHttpFallback<T>(command, args);
    }
  }

  private async callHttpFallback<T>(command: 'GET' | 'SET', args: string[]): Promise<T> {
    const fetcher = this.options.fetchImpl ?? globalThis.fetch;
    if (!fetcher) {
      throw new Error(`WebSocket ${command} failed and no fetch implementation is available for HTTP fallback.`);
    }

    const authHeaders: Record<string, string> = {};
    if (this.options.password) {
      authHeaders.Authorization = `Bearer ${this.options.password}`;
    }
    if (command === 'GET') {
      const response = await fetcher(`${this.options.httpUrl}/get/${encodeURIComponent(args[0])}`, {
        method: 'GET',
        headers: authHeaders
      });
      const body = await response.json() as { result?: string | null; error?: string };
      if (body.error) {
        throw new Error(body.error);
      }
      return body.result as T;
    }

    const response = await fetcher(`${this.options.httpUrl}/set/${encodeURIComponent(args[0])}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...authHeaders },
      body: JSON.stringify({ value: args[1] })
    });
    const body = await response.json() as { result?: string; error?: string };
    if (body.error) {
      throw new Error(body.error);
    }
    return body.result as T;
  }

  private async call(command: string, ...args: string[]): Promise<unknown> {
    return this.callInternal(command, args, true);
  }

  private async callInternal(command: string, args: string[], ensureConnected: boolean): Promise<unknown> {
    if (ensureConnected) {
      await this.connect();
    }

    if (!this.socket || this.socket.readyState !== OPEN_STATE) {
      throw new Error('WebSocket is not connected.');
    }

    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.pending.findIndex((entry) => entry.reject === reject);
        if (idx >= 0) {
          this.pending.splice(idx, 1);
        }
        reject(new Error(`Timed out waiting for ${command} response.`));
      }, this.options.requestTimeoutMs);

      this.pending.push({
        resolve,
        reject,
        timer
      });

      this.socket!.send(JSON.stringify([command, ...args]));
    });
  }

  private async openSocket(): Promise<void> {
    const socket = await this.createSocket(this.options.wsUrl);
    this.socket = socket;

    await new Promise<void>((resolve, reject) => {
      const onOpen = async () => {
        try {
          await this.postOpenHandshake();
          this.reconnectAttempts = 0;
          await this.resubscribe();
          resolve();
        } catch (error) {
          reject(error);
        }
      };

      const onMessage = (eventOrData: unknown) => {
        const raw = this.extractMessageData(eventOrData);
        if (typeof raw !== 'string') {
          return;
        }
        this.handleMessage(raw);
      };

      const onError = () => {
        reject(new Error('WebSocket connection failed.'));
      };

      const onClose = () => {
        this.socket = null;
        this.rejectPending(new Error('WebSocket closed.'));
        if (!this.manuallyClosed && this.options.reconnect) {
          this.scheduleReconnect();
        }
      };

      this.bind(socket, 'open', onOpen);
      this.bind(socket, 'message', onMessage);
      this.bind(socket, 'error', onError);
      this.bind(socket, 'close', onClose);
    });
  }

  private async createSocket(url: string): Promise<WebSocketLike> {
    if (this.options.webSocketFactory) {
      return this.options.webSocketFactory(url);
    }

    const globalCtor = (globalThis as unknown as { WebSocket?: new (u: string) => WebSocketLike }).WebSocket;
    if (globalCtor) {
      return new globalCtor(url);
    }

    const module = await import('ws');
    const NodeWs = module.default;
    return new NodeWs(url) as unknown as WebSocketLike;
  }

  private async postOpenHandshake(): Promise<void> {
    if (this.options.password) {
      await this.callInternal('AUTH', [this.options.password], false);
    }
    await this.callInternal('HELLO', ['3'], false);
  }

  private async resubscribe(): Promise<void> {
    for (const channel of this.channelHandlers.keys()) {
      await this.callInternal('SUBSCRIBE', [channel], false);
    }
    for (const pattern of this.patternHandlers.keys()) {
      await this.callInternal('PSUBSCRIBE', [pattern], false);
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) {
      return;
    }

    const delay = Math.min(
      this.options.reconnectBaseDelayMs * Math.pow(2, this.reconnectAttempts),
      this.options.reconnectMaxDelayMs
    );

    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch(() => {
        if (!this.manuallyClosed && this.options.reconnect) {
          this.scheduleReconnect();
        }
      });
    }, delay);
  }

  private handleMessage(raw: string): void {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    if (parsed.error) {
      const pending = this.pending.shift();
      if (!pending) {
        return;
      }
      clearTimeout(pending.timer);
      pending.reject(new Error(String(parsed.error)));
      return;
    }

    if (this.isPushFrame(parsed)) {
      this.handlePush(parsed.type === 'push' ? parsed.data : parsed.push);
      return;
    }

    const pending = this.pending.shift();
    if (!pending) {
      return;
    }
    clearTimeout(pending.timer);
    pending.resolve(parsed);
  }

  private handlePush(payload: unknown): void {
    if (!Array.isArray(payload) || payload.length < 3) {
      return;
    }

    const type = String(payload[0]).toLowerCase();
    if (type === 'message') {
      const channel = String(payload[1]);
      const body = String(payload[2]);
      this.channelHandlers.get(channel)?.(channel, body);
      return;
    }

    if (type === 'pmessage' && payload.length >= 4) {
      const pattern = String(payload[1]);
      const channel = String(payload[2]);
      const body = String(payload[3]);
      this.patternHandlers.get(pattern)?.(pattern, channel, body);
    }
  }

  private isPushFrame(payload: Record<string, unknown>): boolean {
    return payload.type === 'push' || Array.isArray(payload.push);
  }

  private rejectPending(error: Error): void {
    while (this.pending.length > 0) {
      const pending = this.pending.shift();
      if (!pending) {
        continue;
      }
      clearTimeout(pending.timer);
      pending.reject(error);
    }
  }

  private bind(socket: WebSocketLike, event: string, listener: (data?: unknown) => void): void {
    if (socket.addEventListener) {
      socket.addEventListener(event, listener as (event: { data?: unknown }) => void);
      return;
    }

    socket.on?.(event, listener);
  }

  private extractMessageData(eventOrData: unknown): unknown {
    if (typeof eventOrData === 'string') {
      return eventOrData;
    }

    if (eventOrData && typeof eventOrData === 'object' && 'data' in (eventOrData as { data?: unknown })) {
      const evt = eventOrData as { data?: unknown };
      if (typeof evt.data === 'string') {
        return evt.data;
      }
      if (evt.data instanceof ArrayBuffer) {
        return new TextDecoder().decode(evt.data);
      }
    }

    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(eventOrData)) {
      return eventOrData.toString('utf8');
    }

    return undefined;
  }

  private toSetArgs(options?: SetOptions): string[] {
    if (!options) {
      return [];
    }

    const args: string[] = [];
    if (typeof options.ex === 'number') {
      args.push('EX', String(options.ex));
    }
    if (typeof options.px === 'number') {
      args.push('PX', String(options.px));
    }
    if (options.nx) {
      args.push('NX');
    }
    if (options.xx) {
      args.push('XX');
    }
    if (options.keepttl) {
      args.push('KEEPTTL');
    }
    return args;
  }

  private asStringArray(payload: unknown): string[] {
    if (!Array.isArray(payload)) {
      return [];
    }
    return payload.map((value) => (value === null || value === undefined ? '' : String(value)));
  }

  private asNumber(payload: unknown): number {
    return Number(payload);
  }
}

export const createClient = (options?: ConnectOptions): SwarmKeyDbClient => new SwarmKeyDbClient(options);
