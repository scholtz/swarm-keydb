# JavaScript/TypeScript SDK (`@swarm-keydb/client`)

## Installation

```bash
npm install @swarm-keydb/client
```

## 5-line quick start

```ts
import { createClient } from '@swarm-keydb/client';

const client = createClient({ wsUrl: 'ws://127.0.0.1:8765/' });
await client.connect();
await client.set('hello', 'world');
console.log(await client.get('hello'));
```

## Connection options

- `wsUrl` (default `ws://127.0.0.1:8765/`): primary WebSocket endpoint
- `httpUrl` (default `http://127.0.0.1:8080`): REST fallback endpoint
- `password`: sends `AUTH` automatically after connect
- `reconnect` + `reconnectBaseDelayMs` + `reconnectMaxDelayMs`: exponential backoff reconnect
- `requestTimeoutMs`: per-command timeout
- `httpFallback`: use HTTP fallback for `get`/`set` when WebSocket is unavailable

The client negotiates RESP3 automatically by sending `HELLO 3` after connect.

## API reference (TypeScript)

- Core: `get`, `set`, `del`, `exists`, `expire`, `ttl`, `keys`, `mget`, `mset`, `incr`, `decr`, `incrby`
- Hash: `hget`, `hset`, `hgetall`, `hdel`, `hkeys`, `hvals`
- List: `lpush`, `rpush`, `lrange`, `llen`, `lpop`, `rpop`
- Set: `sadd`, `smembers`, `sismember`, `srem`, `scard`
- Sorted set: `zadd`, `zrange`, `zscore`, `zrank`, `zrem`
- Pub/Sub: `subscribe`, `unsubscribe`, `psubscribe`, `punsubscribe`, `publish`
- Streams: `xadd`, `xrange`, `xread`, `xlen`

## Migration from ioredis / node-redis

Common operations map directly:

- `redis.get(key)` -> `client.get(key)`
- `redis.set(key, value)` -> `client.set(key, value)`
- `redis.del(key)` -> `client.del(key)`
- `redis.subscribe(channel, cb)` -> `client.subscribe(channel, cb)`
- `redis.publish(channel, message)` -> `client.publish(channel, message)`

Differences:

- Transport is WebSocket-first (plus optional HTTP fallback) rather than TCP Redis.
- The SDK handles `HELLO 3` negotiation automatically.

## Bundle size and CSP notes

- Runtime dependency footprint is intentionally small (`ws` in Node.js environments).
- Browser usage relies on the native `WebSocket` and `fetch` APIs.
- No `eval`/dynamic code execution is required by the SDK runtime.
