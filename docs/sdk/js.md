# JavaScript SDK Quick Reference

Install:

```bash
cd swarm-keydb-js
npm ci
```

Run a complete example:

```bash
node examples/user-profile.mjs
```

Core API:

- `connect()`, `disconnect()`
- `put(key, value)`, `get(key)`, `delete(key)`
- `list(pattern?)`
- `batchPut(entries)`, `batchGet(keys)`
- `setWithTTL(key, value, ttlSeconds)`

Error handling note: when the server rejects a command (invalid TTL, ACL denied, integrity/read failure), SDK methods reject with the Redis/server error.
