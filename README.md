# swarm-keydb

A small C# key-value database that speaks the Redis RESP protocol and stores values as Swarm objects.

## Features

- Redis-compatible commands for `PING`, `SET`, `SETEX`, `PSETEX`, `GET`, `MGET`, `MSET`, `MSETNX`, `DEL`, `MDEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `SCAN`, `TYPE`, and `QUIT`.
- String, JSON, and binary value helpers in the `SwarmKeyDbClient` library.
- Key listing and cursor-based iteration.
- Bee HTTP API storage with postage batch configuration handled by environment variables.
- Local file storage backend for development and tests.
- Docker build for running the Redis-compatible server.

## Build and test

```bash
dotnet build SwarmKeyDb.slnx
dotnet run --project tests/SwarmKeyDb.Tests/SwarmKeyDb.Tests.csproj
```

## Run locally

The default backend stores Swarm-like content-addressed blobs on disk so the Redis protocol can be tested without a Bee node:

```bash
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
redis-cli -p 6379 SET profile:name Ada
redis-cli -p 6379 GET profile:name
redis-cli -p 6379 KEYS '*'
```

## Redis command examples (RESP responses)

```text
SETEX session:token 300 abc123    -> +OK
TTL session:token                 -> :<1..300>
MSET a 1 b 2 c 3                  -> +OK
MGET a b missing                  -> *3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1
PERSIST session:token             -> :1 (or :0 when no TTL exists)
SET profile:name Ada EX 60        -> +OK
```

## Run against Bee/Swarm

Set the backend to `bee` and provide the Bee API endpoint and postage batch id. Uploads automatically include the configured postage batch and pin the uploaded object.

```bash
export SWARM_KEYDB_BACKEND=bee
export BEE_URL=http://localhost:1633/
export BEE_POSTAGE_BATCH_ID=<your-postage-batch-id>
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

The key index is persisted in `SWARM_KEYDB_DATA_DIR/index.json` and values are fetched from the Swarm references stored there.

## Docker

```bash
docker build -t swarm-keydb .
docker run --rm -p 6379:6379 -v swarm-keydb-data:/data swarm-keydb
```

For Bee-backed storage:

```bash
docker run --rm -p 6379:6379 \
  -e SWARM_KEYDB_BACKEND=bee \
  -e BEE_URL=http://host.docker.internal:1633/ \
  -e BEE_POSTAGE_BATCH_ID=<your-postage-batch-id> \
  -v swarm-keydb-data:/data \
  swarm-keydb
```

## Library example

```csharp
using SwarmKeyDb;

var swarm = new BeeSwarmClient(new Uri("http://localhost:1633/"), postageBatchId);
var index = new FileKeyIndex(".swarm-keydb/index.json");
var db = new SwarmKeyDbClient(new SwarmKeyValueStore(swarm, index));

await db.PutStringAsync("profile:name", "Ada");
await db.PutJsonAsync("profile:settings", new { Theme = "dark" });
await db.PutBytesAsync("profile:avatar", avatarBytes);

Console.WriteLine(await db.GetStringAsync("profile:name"));
foreach (var key in await db.KeysAsync())
{
    Console.WriteLine(key);
}
```
