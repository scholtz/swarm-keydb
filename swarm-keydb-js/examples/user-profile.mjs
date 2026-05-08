import { SwarmKeyDb } from '../index.js';

const db = new SwarmKeyDb({ host: '127.0.0.1', port: 6379 });
await db.connect();
await db.put('user:42', JSON.stringify({ name: 'Ada', role: 'admin' }));
try {
  console.log(await db.get('user:42'));
} catch (error) {
  console.error('Failed to read user:42:', error.message);
}
await db.disconnect();
