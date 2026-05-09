# Getting Started (5 minutes)

This guide gets you from zero to a working `put`/`get` round-trip in minutes.

> [!IMPORTANT]
> If you run with Bee storage (`SWARM_KEYDB_BACKEND=bee`), you must provide a valid postage batch id. See `docs/deployment.md`.

## Prerequisites

- .NET 10 SDK
- Docker (optional, for containerized run)
- Bee node URL and postage batch id for Bee-backed storage (`BEE_URL`, `BEE_POSTAGE_BATCH_ID`)

## Quickstart (local backend)

```bash
# 1) start the server
 dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
# 2) in another terminal, write and read
 redis-cli -p 6379 SET demo:key "hello swarm"
 redis-cli -p 6379 GET demo:key
```

Expected output:

```text
OK
"hello swarm"
```

## Bee-backed quickstart

```bash
export SWARM_KEYDB_BACKEND=bee
export BEE_URL=http://localhost:1633/
export BEE_POSTAGE_BATCH_ID=<your-postage-batch-id>
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

Then run the same `SET`/`GET` commands above.

## Stream quickstart (`XADD` + `XRANGE`)

```bash
redis-cli -p 6379 XADD events * type created user alice
redis-cli -p 6379 XADD events * type updated user alice
redis-cli -p 6379 XRANGE events - +
redis-cli -p 6379 XREVRANGE events + - COUNT 1
redis-cli -p 6379 XLEN events
```

Expected shape:

```text
<ms>-<seq>
1) 1) "<ms>-<seq>"
   2) 1) "type"
      2) "created"
      3) "user"
      4) "alice"
...
```

## IPFS-backed quickstart

```bash
export BACKEND=ipfs
export IPFS_API_URL=http://localhost:5001/
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

## Verify from C# client

```bash
dotnet run --project examples/UserProfileExample/UserProfileExample.csproj
```

## Troubleshooting

- `ERR invalid expire seconds`: TTL must be a positive integer.
- `ERR invalid expire milliseconds`: millisecond TTL must be a positive integer.
- `ERR Access denied...`: authenticate with `AUTHADDR <0x-address>` when ACL is enabled.
- `DataIntegrityException`: stored bytes failed integrity verification; re-write the key or inspect storage.
- Bee upload/readiness failures: confirm Bee is reachable and `BEE_POSTAGE_BATCH_ID` is valid.
