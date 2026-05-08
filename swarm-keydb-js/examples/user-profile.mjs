import { SwarmKeyDb } from '../index.js';

const db = new SwarmKeyDb({ host: '127.0.0.1', port: 6379 });
await db.connect();
await db.put('user:42', JSON.stringify({ name: 'Ada', role: 'admin' }));
console.log(await db.get('user:42'));
await db.disconnect();
