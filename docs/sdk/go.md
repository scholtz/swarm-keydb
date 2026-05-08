# Go SDK Quick Reference

Install dependencies and test:

```bash
cd swarm-keydb-go
go test ./...
```

Run a complete example:

```bash
go run ./examples/user-profile
```

Core API:

- `Put(ctx, key, value)`, `Get(ctx, key)`, `Delete(ctx, key)`
- `List(ctx, pattern)`
- `BatchPut(ctx, entries)`, `BatchGet(ctx, keys)`
- `SetWithTTL(ctx, key, value, ttlSeconds)`
- `Backup(ctx)`, `Restore(ctx, ref, key)`, `RotateKey(ctx, oldKey, newKey)`
- `PutJSON` / `GetJSON`

Error handling note: Redis/server errors are returned directly (`error`) for invalid TTL, ACL denial, and failed reads.
