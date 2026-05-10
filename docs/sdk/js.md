# JavaScript SDK Quick Reference

Use the new npm SDK package:

- Package: `@swarm-keydb/client`
- Source in this repository: `sdk/js/`
- Full documentation: `docs/js-sdk.md`

Quick start:

```ts
import { createClient } from '@swarm-keydb/client';

const client = createClient({ wsUrl: 'ws://127.0.0.1:8765/' });
await client.connect();
await client.set('hello', 'world');
console.log(await client.get('hello'));
```
