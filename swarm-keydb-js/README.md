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
- `backup()`
- `restore(ref, key?)`
- `rotateKey(oldKey, newKey)`

## Privacy-preserving mode

```ts
import { PrivacyMode, SwarmKeyDb } from 'swarm-keydb-js';

const db = new SwarmKeyDb({
  host: '127.0.0.1',
  port: 6379,
  privacyMode: PrivacyMode.ObliviousHashing,
  privacyKey: '<64-char-hex-key>'
});
```

## Offline-first mode

```ts
import { OfflineMode, SwarmKeyDb } from 'swarm-keydb-js';

const db = new SwarmKeyDb({
  host: '127.0.0.1',
  port: 6379,
  offlineMode: OfflineMode.Auto
});
```

Use this with a server started in offline-first mode so queued writes and cached reads stay enabled during Bee outages.

## Data integrity

The SwarmKeyDb server verifies a SHA-256 integrity envelope on every read by default. If stored Swarm data has been corrupted or tampered with, `get()`/`batchGet()` reject with the wrapped Redis/server error; handle that error path the same way you would handle any failed Redis read.

## Using IPFS and hybrid backends

The SDK API is unchanged. Start the server with either:

```bash
BACKEND=ipfs IPFS_API_URL=http://localhost:5001/ dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
BACKEND=hybrid BEE_URL=http://localhost:1633/ BEE_POSTAGE_BATCH_ID=<id> IPFS_API_URL=http://localhost:5001/ dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

## Examples

- `examples/user-profile.mjs`
- `examples/config-management.mjs`
- `examples/chat-message-history.mjs`

## Test

```bash
npm install
npm test
```
