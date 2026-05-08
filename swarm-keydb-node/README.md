# swarm-keydb-node

Node.js middleware and service helpers for SwarmKeyDb.

## Install

```bash
npm install swarm-keydb-node swarm-keydb-js
```

## Quick start

```js
import express from 'express';
import { SwarmKeyDbService, createExpressCacheMiddleware } from 'swarm-keydb-node';

const service = new SwarmKeyDbService({ clientOptions: { host: '127.0.0.1', port: 6379 } });
await service.initialize();

const app = express();
app.use(express.json());
app.use(createExpressCacheMiddleware(service));

app.get('/profile/:id', async (req, res) => {
  const key = `profile:${req.params.id}`;
  const value = await service.get(key);
  res.json({ key, value });
});

app.listen(3000, () => console.log('Listening on :3000'));
```

## API

- `SwarmKeyDbService` with connection pooling and retry logic.
- `createExpressSwarmKeyDbMiddleware(service, options?)` for session/value attachment.
- `createExpressCacheMiddleware(service, options?)` for response caching.
- `createFastifySwarmKeyDbPlugin(service, options?)` plugin for Fastify request decoration.
- `service.scan(prefix?, batchSize?)` async iterator for large keyspace scans.

## Test

```bash
npm install
npm test
```
