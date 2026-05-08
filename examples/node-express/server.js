import express from 'express';
import {
  SwarmKeyDbService,
  createExpressCacheMiddleware,
  createExpressSwarmKeyDbMiddleware
} from '../../swarm-keydb-node/index.js';

const app = express();
app.use(express.json());

const service = new SwarmKeyDbService({ clientOptions: { host: '127.0.0.1', port: 6379 } });
await service.initialize();

app.use(createExpressSwarmKeyDbMiddleware(service, {
  sessionKeyResolver: (req) => req.headers['x-swarm-session-key']
}));
app.use(createExpressCacheMiddleware(service));

app.put('/profile/:id', async (req, res, next) => {
  try {
    const key = `profile:${req.params.id}`;
    await service.put(key, req.body);
    res.json({ ok: true, key });
  } catch (error) {
    next(error);
  }
});

app.get('/profile/:id', async (req, res, next) => {
  try {
    const key = `profile:${req.params.id}`;
    const value = await service.get(key);
    res.json({ key, value, session: req.swarmSession ?? null });
  } catch (error) {
    next(error);
  }
});

app.delete('/profile/:id', async (req, res, next) => {
  try {
    const key = `profile:${req.params.id}`;
    const deleted = await service.delete(key);
    res.json({ key, deleted });
  } catch (error) {
    next(error);
  }
});

app.get('/profile', async (_req, res, next) => {
  try {
    const keys = [];
    for await (const key of service.scan('profile:', 50)) {
      keys.push(key);
    }

    res.json({ keys });
  } catch (error) {
    next(error);
  }
});

const server = app.listen(3001, () => {
  console.log('Node/Express SwarmKeyDb example listening on http://127.0.0.1:3001');
});

process.on('SIGINT', async () => {
  server.close();
  await service.dispose();
  process.exit(0);
});
