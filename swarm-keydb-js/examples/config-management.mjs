import { SwarmKeyDb } from '../index.js';

const db = new SwarmKeyDb({ host: '127.0.0.1', port: 6379 });
await db.connect();
await db.batchPut({ 'config:theme': 'dark', 'config:region': 'eu-west-1' });
console.log(await db.batchGet(['config:theme', 'config:region']));
await db.disconnect();
