# Node.js connector quick reference

Install:

```bash
cd swarm-keydb-node
npm install
```

Run tests:

```bash
npm test
```

Core API:

- `SwarmKeyDbService` (pooling + retries)
- `service.scan(prefix?, batchSize?)` async iterator
- `createExpressSwarmKeyDbMiddleware(service, options?)`
- `createExpressCacheMiddleware(service, options?)`
- `createFastifySwarmKeyDbPlugin(service, options?)`

Example app:

```bash
cd examples/node-express
npm install
npm start
```
