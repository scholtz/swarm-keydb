# @swarm-keydb/client

TypeScript-first JavaScript SDK for SwarmKeyDb over WebSocket (primary) with HTTP fallback.

## Install

```bash
npm install @swarm-keydb/client
```

## Quick start

```ts
import { createClient } from '@swarm-keydb/client';

const client = createClient({ wsUrl: 'ws://127.0.0.1:8765/' });
await client.connect();
await client.set('hello', 'world');
console.log(await client.get('hello')); // world
await client.disconnect();
```

## API highlights

- Core: `get`, `set`, `del`, `exists`, `expire`, `ttl`, `keys`, `mget`, `mset`, `incr`, `decr`, `incrby`
- Hashes: `hget`, `hset`, `hgetall`, `hdel`, `hkeys`, `hvals`
- Lists: `lpush`, `rpush`, `lrange`, `llen`, `lpop`, `rpop`
- Sets: `sadd`, `smembers`, `sismember`, `srem`, `scard`
- Sorted sets: `zadd`, `zrange`, `zscore`, `zrank`, `zrem`
- Pub/Sub: `subscribe`, `unsubscribe`, `psubscribe`, `punsubscribe`, `publish`
- Streams: `xadd`, `xrange`, `xread`, `xlen`

## Build and test

```bash
npm run build
npm test
npm run typecheck
```
