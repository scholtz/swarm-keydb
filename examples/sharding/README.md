# Sharded SwarmKeyDb example (3 shards)

Start the sharded server:

```bash
cd examples/sharding
docker compose up --build -d
```

Write/read a few keys:

```bash
redis-cli -p 6379 MSET user:1 alice user:2 bob user:3 carol
redis-cli -p 6379 MGET user:1 user:2 user:3
redis-cli -p 6379 SCAN 0 COUNT 100
```

Inspect shard health and metrics:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:9090/metrics | grep swarmkeydb_shard_
```

Stop:

```bash
docker compose down -v
```

## Manual rebalancing (v1)

When changing shard topology (adding/removing nodes):

1. Deploy with both old and new shard configuration in parallel.
2. Iterate keys via `SCAN` and rewrite them (`GET` + `SET`) through the new topology.
3. Verify `/health`, `/ready`, and `swarmkeydb_shard_*` metrics are healthy.
4. Decommission old shard configuration after migration validation.
