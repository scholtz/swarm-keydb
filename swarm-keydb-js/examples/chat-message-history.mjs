import { SwarmKeyDb } from '../index.js';

const db = new SwarmKeyDb({ host: '127.0.0.1', port: 6379 });
await db.connect();
await db.setWithTTL('chat:room1:last', JSON.stringify({ from: 'alice', text: 'hi' }), 60);
console.log(await db.list('chat:*'));
await db.disconnect();
