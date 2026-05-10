import express from 'express';
import { createClient } from '../../../sdk/js/dist/index.js';

const app = express();
app.use(express.json());
const cache = createClient({ wsUrl: process.env.SWARM_KEYDB_WS_URL ?? 'ws://127.0.0.1:8765/' });
await cache.connect();

app.get('/session/:id', async (req, res) => {
  const value = await cache.get(`session:${req.params.id}`);
  res.json({ value });
});

app.post('/session/:id', async (req, res) => {
  await cache.set(`session:${req.params.id}`, JSON.stringify(req.body));
  res.json({ ok: true });
});

app.listen(3000, () => console.log('nodejs-cache demo on :3000'));
