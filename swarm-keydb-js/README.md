# swarm-keydb-js

JavaScript/TypeScript SDK for SwarmKeyDb via the Redis protocol.

## Quickstart

```ts
import { SwarmKeyDb } from 'swarm-keydb-js';

const db = new SwarmKeyDb({ host: '127.0.0.1', port: 6379 });
await db.connect();
await db.put('hello', 'world');
console.log(await db.get('hello'));
await db.disconnect();
```

## API

- `get(key)`
- `put(key, value)`
- `delete(key)`
- `list(pattern?)`
- `batchGet(keys)`
- `batchPut(entries)`
- `setWithTTL(key, value, ttlSeconds)`

## Examples

- `examples/user-profile.mjs`
- `examples/config-management.mjs`
- `examples/chat-message-history.mjs`

## Test

```bash
npm install
npm test
```
