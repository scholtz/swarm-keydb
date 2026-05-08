# swarm-keydb

A small C# key-value database that speaks the Redis RESP protocol and stores values as Swarm objects.

## Features

- Redis-compatible commands for `PING`, `SET`, `SETEX`, `PSETEX`, `GET`, `MGET`, `MSET`, `MSETNX`, `DEL`, `MDEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `SCAN`, `TYPE`, and `QUIT`.
- Connection-scoped Ethereum-address ACL enforcement via `AUTHADDR` for shared databases.
- String, JSON, and binary value helpers in the `SwarmKeyDbClient` library.
- CRDT-backed conflict resolution (LWW register by default, with OR-Set and PN-counter strategies available).
- Prefix scans, lexicographic range scans, predicate queries, and cursor-based iteration.
- Bee HTTP API storage with postage batch configuration handled by environment variables.
- Local file storage backend for development and tests.
- `skdb` CLI for database management and debugging from the terminal.
- .NET 10 build, Docker packaging, and Kubernetes deployment manifests.

## Documentation

Project documentation lives under `docs/`:

- `docs/README.md`
- `docs/architecture/README.md`
- `docs/development/README.md`
- `docs/deployment/README.md`
- `docs/reference/configuration.md`

## Build and test

```bash
dotnet build SwarmKeyDb.slnx
dotnet run --project tests/SwarmKeyDb.Tests/SwarmKeyDb.Tests.csproj
```

## Multi-language SDKs

- `swarm-keydb-js/` - JavaScript/TypeScript SDK (`get`, `put`, `delete`, `list`, `batchGet`, `batchPut`, `setWithTTL`)
- `swarm-keydb-py/` - Python SDK with sync and async clients
- `swarm-keydb-go/` - Go SDK with context-aware API and JSON helpers

SDK test commands:

```bash
(cd swarm-keydb-js && npm install && npm test)
(cd swarm-keydb-py && pip install . && python -m unittest discover -s tests -v)
(cd swarm-keydb-go && go test ./...)
```

## CLI (`skdb`)

Install as a .NET tool:

```bash
dotnet pack src/SwarmKeyDb.Cli/SwarmKeyDb.Cli.csproj -c Release
dotnet tool install -g SwarmKeyDb.Cli --add-source src/SwarmKeyDb.Cli/bin/Release
```

Configure Bee once, then use the CLI commands:

```bash
skdb config set --bee-url http://localhost:1633/ --batch-id <your-postage-batch-id>
skdb put user:alice '{"name":"Alice","role":"admin"}'
skdb get user:alice
skdb list --prefix user:
skdb scan --from user:a --to user:z
skdb delete user:alice
skdb stats
```

Global overrides:

- `--bee-url`, `--batch-id`, `--output plain|json|table`
- `SWARMKEYDB_BEE_URL`, `SWARMKEYDB_BATCH_ID`, `SWARMKEYDB_OUTPUT`

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

## Querying

```csharp
using System.Text;
using SwarmKeyDb;

var swarm = new BeeSwarmClient(new Uri("http://localhost:1633/"), postageBatchId);
var index = new FileKeyIndex(".swarm-keydb/index.json");
var db = new SwarmKeyDbClient(new SwarmKeyValueStore(swarm, index));

await db.PutStringAsync("orders:0001", "paid");
await db.PutStringAsync("orders:0002", "pending");
await db.PutStringAsync("profile:alice", "active");

var prefixKeys = await db.GetKeysWithPrefixAsync("orders:");
var range = await db.GetKeyRangeAsync("orders:0001", "orders:9999", new RangeScanOptions { IncludeValues = true });

var scan = await db.ScanAsync(null, 2);
while (!string.IsNullOrEmpty(scan.NextCursor))
{
    scan = await db.ScanAsync(scan.NextCursor, 2);
}

await foreach (var item in db.QueryAsync(
                   key => key.StartsWith("orders:", StringComparison.Ordinal),
                   value => Encoding.UTF8.GetString(value).Contains("paid", StringComparison.Ordinal)))
{
    Console.WriteLine($"{item.Key} -> {Encoding.UTF8.GetString(item.Value)}");
}
```

## Run against Bee/Swarm

Set the backend to `bee` and provide the Bee API endpoint and postage batch id. Uploads automatically include the configured postage batch and pin the uploaded object.

```bash
export SWARM_KEYDB_BACKEND=bee
export BEE_URL=http://localhost:1633/
export BEE_POSTAGE_BATCH_ID=<your-postage-batch-id>
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

The checked-in Docker Compose and Kubernetes manifests default to a Bee Sepolia testnet setup. Replace the RPC endpoint, Bee password, and postage batch id placeholders before use.

The key index is persisted in `SWARM_KEYDB_DATA_DIR/index.json` and values are fetched from the Swarm references stored there.

### In-memory read cache

The server enables an in-memory read-through cache by default for hot keys. Configure it with:

- `SWARM_KEYDB_CACHE_ENABLED` (`true`/`false`, default `true`)
- `SWARM_KEYDB_CACHE_MAX_ENTRIES` (default `1000`)
- `SWARM_KEYDB_CACHE_DEFAULT_TTL_SECONDS` (optional cap for cache-entry lifetime)

Writes (`SET`, `SETEX`, `MSET`, etc.), deletes, and TTL changes invalidate cached entries so subsequent reads refresh from Swarm/index data.

### Async high-throughput write queue

The server can process write operations asynchronously through an internal queue with configurable batching and concurrency:

- `SWARM_KEYDB_ASYNC_ENABLED` (`true`/`false`, default `true`)
- `SWARM_KEYDB_MAX_CONCURRENT_WRITES` (default `4`)
- `SWARM_KEYDB_WRITE_BATCH_SIZE` (default `64`)
- `SWARM_KEYDB_BATCH_FLUSH_INTERVAL_MS` (default `100`)

### Monitoring and observability

SwarmKeyDb now exposes production observability endpoints:

- `GET /metrics` (Prometheus text format, default port `9090`)
- `GET /health` (liveness)
- `GET /ready` (Bee connectivity + postage batch validation when using `SWARM_KEYDB_BACKEND=bee`)
- `GET /dashboard` (lightweight HTML dashboard, default port `8080`)
- `GET /logs` (recent structured command logs with correlation IDs)

Configuration (environment variables override `appsettings.json`):

- `METRICS_ENABLED` (`true`/`false`, default `true`)
- `METRICS_PORT` (default `9090`)
- `DASHBOARD_ENABLED` (`true`/`false`, default `true`)
- `DASHBOARD_PORT` (default `8080`)
- `LOG_LEVEL` (`Debug`, `Information`, `Warning`, `Error`; default `Information`)

Monitoring endpoints bind to the same host as Redis (`SWARM_KEYDB_BIND`, default `0.0.0.0`). For local-only exposure, set `SWARM_KEYDB_BIND=127.0.0.1`.

Quick check:

```bash
curl http://localhost:9090/metrics
curl http://localhost:8080/health
curl http://localhost:8080/ready
open http://localhost:8080/dashboard
```

Prometheus scrape example:

```yaml
scrape_configs:
  - job_name: swarm-keydb
    metrics_path: /metrics
    static_configs:
      - targets: ['swarm-keydb:9090']
```

Grafana panel JSON example (import into a dashboard panel):

```json
{
  "title": "SwarmKeyDb GET ops/sec",
  "type": "timeseries",
  "targets": [
    {
      "expr": "rate(swarmkeydb_operations_total{operation=\"get\",status=\"success\"}[1m])",
      "legendFormat": "GET ops/sec"
    }
  ]
}
```

### Compression

The server supports transparent value compression to reduce Swarm storage costs and improve transfer latency. Configure it with:

- `SWARM_KEYDB_COMPRESSION_ENABLED` (`true`/`false`, default `false`)
- `SWARM_KEYDB_COMPRESSION_ALGORITHM` (`GZip` or `Brotli`, default `GZip`)
- `SWARM_KEYDB_COMPRESSION_MIN_SIZE_BYTES` (minimum value size to compress, default `64`)

**Algorithm guidance:**

| Algorithm | Use when |
|-----------|----------|
| `GZip`    | General-purpose; best compatibility; slightly faster compression/decompression |
| `Brotli`  | Better compression ratio for text-heavy payloads (JSON, HTML, configs); slightly slower |

**Backward compatibility:** Values stored before compression was enabled are returned unchanged. The store detects compressed values by their magic-byte header (`0x1F 0x8B` for GZip, `0xCE 0xB8` for Brotli) and decompresses automatically; raw legacy bytes pass through as-is.

**Example (Docker):**

```bash
docker run --rm -p 6379:6379 \
  -e SWARM_KEYDB_COMPRESSION_ENABLED=true \
  -e SWARM_KEYDB_COMPRESSION_ALGORITHM=GZip \
  -e SWARM_KEYDB_COMPRESSION_MIN_SIZE_BYTES=64 \
  -v swarm-keydb-data:/data \
  swarm-keydb
```

### Encryption

The server supports transparent end-to-end encryption (AES-256-GCM) for all values stored in Swarm. Only a client holding the correct key can read the data — Swarm node operators and network observers see only ciphertext.

Configure it with:

- `SWARM_KEYDB_ENCRYPTION_ENABLED` (`true`/`false`, default `false`)
- `SWARM_KEYDB_ENCRYPTION_KEY` — 32-byte AES-256 key as a 64-character hex string (preferred for server deployments)
- `SWARM_KEYDB_ENCRYPTION_ETH_KEY` — Ethereum private key as a 64-character hex string; the AES key is derived from it using HKDF-SHA256 (convenient for dApps where the user's wallet is the identity)

**Security model:**

- Each write generates a fresh random 12-byte nonce — identical values produce different ciphertext on every write.
- The GCM authentication tag (16 bytes) provides integrity protection; tampered ciphertext causes a `CryptographicException` on read.
- Encrypted blobs are identified by a 2-byte magic header (`0xAE 0x73`); unencrypted legacy values are returned unchanged for backward compatibility.
- Key names (Redis keys) are **not** encrypted in this release — only values.

**Startup behaviour:** If `SWARM_KEYDB_ENCRYPTION_ENABLED=true` but neither `SWARM_KEYDB_ENCRYPTION_KEY` nor `SWARM_KEYDB_ENCRYPTION_ETH_KEY` is set, the server fails fast with a descriptive error — it will never silently store plaintext when encryption is expected.

**Layer ordering:** The configured stack is `Cache → CRDT → Compress → Encrypt → ACL → Swarm` (outermost to innermost), so CRDT merges run on plaintext while persisted Swarm bytes remain encrypted.
This includes CRDT metadata (`vectorClock`, `timestamp`, and strategy marker), because the full CRDT envelope is encrypted before being written to Swarm.

**Example (Docker):**

```bash
# Generate a random 32-byte key:
openssl rand -hex 32

docker run --rm -p 6379:6379 \
  -e SWARM_KEYDB_ENCRYPTION_ENABLED=true \
  -e SWARM_KEYDB_ENCRYPTION_KEY=<64-char-hex-key> \
  -v swarm-keydb-data:/data \
  swarm-keydb
```

**Ethereum keypair–derived key (developer-friendly):**

```bash
docker run --rm -p 6379:6379 \
  -e SWARM_KEYDB_ENCRYPTION_ENABLED=true \
  -e SWARM_KEYDB_ENCRYPTION_ETH_KEY=<64-char-hex-ethereum-private-key> \
  -v swarm-keydb-data:/data \
  swarm-keydb
```

**Round-trip example:**

```bash
redis-cli -p 6379 SET profile:name Ada
# Value is stored as AES-256-GCM ciphertext in Swarm; raw Swarm bytes are unreadable.
redis-cli -p 6379 GET profile:name
# Returns: "Ada"  (decrypted transparently by the server)
```

### Access control lists (ACLs)

The server supports Ethereum-address-based access control for shared databases. When ACLs are enabled, reads (`GET`, `MGET`, `KEYS`, `SCAN`, `TYPE`, `TTL`, `PTTL`) require read permission, and writes (`SET`, `SETEX`, `PSETEX`, `MSET`, `MSETNX`, `DEL`, `MDEL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PERSIST`) require write permission. `admin` grants both.

Configure it with:

- `SWARM_KEYDB_ACL_ENABLED` (`true`/`false`, default `false`)
- `SWARM_KEYDB_ACL_MODE` (`allowlist` or `denylist`, default `allowlist`)
- `SWARM_KEYDB_ACL_ENTRIES` — JSON array of ACL entries such as `[{"address":"0x1111111111111111111111111111111111111111","permission":"admin"}]`

**Modes:**

- `allowlist`: only listed addresses may access the database, with the permissions granted in `SWARM_KEYDB_ACL_ENTRIES`
- `denylist`: all addresses are allowed except listed addresses, which are denied for the listed permission (`read`, `write`, or `admin`)

**Startup behaviour:** If `SWARM_KEYDB_ACL_ENABLED=true` and `SWARM_KEYDB_ACL_ENTRIES` is empty or invalid, the server fails fast with a descriptive error.

**Layer ordering:** The configured stack is `Cache → CRDT → Compress → Encrypt → ACL → Swarm` (outermost to innermost), so ACL checks are enforced immediately before Swarm storage access.

**Supplying caller identity:** SwarmKeyDb currently speaks Redis RESP over TCP, so there is no HTTP header transport on the wire. For the Redis server, identify the caller once per connection with `AUTHADDR <0x-address>`. HTTP adapters can map the same identity to an `X-Eth-Address` header and translate `AccessDeniedException` to HTTP `403`.

**Example (allowlist):**

```bash
export SWARM_KEYDB_ACL_ENABLED=true
export SWARM_KEYDB_ACL_MODE=allowlist
export SWARM_KEYDB_ACL_ENTRIES='[
  {"address":"0x1111111111111111111111111111111111111111","permission":"admin"},
  {"address":"0x2222222222222222222222222222222222222222","permission":"read"}
]'
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

Then, from a Redis client session:

```text
AUTHADDR 0x1111111111111111111111111111111111111111 -> +OK
SET shared:doc hello                               -> +OK
GET shared:doc                                     -> $5\r\nhello
```

An unauthorized caller receives a stable protocol-visible error:

```text
AUTHADDR 0x9999999999999999999999999999999999999999 -> +OK
GET shared:doc                                     -> -ERR Access denied: address 0x9999999999999999999999999999999999999999 does not have read permission.
```

**Example (denylist):**

```bash
export SWARM_KEYDB_ACL_ENABLED=true
export SWARM_KEYDB_ACL_MODE=denylist
export SWARM_KEYDB_ACL_ENTRIES='[
  {"address":"0x3333333333333333333333333333333333333333","permission":"admin"}
]'
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

## Docker

```bash
docker build -t swarm-keydb .
docker run --rm -p 6379:6379 -v swarm-keydb-data:/data swarm-keydb
```

To run SwarmKeyDb with a colocated Bee node, copy `.env.example` to `.env` and start the Compose stack:

```bash
docker compose up --build
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

### CRDT merge example

```csharp
await db.SetKeyOptionsAsync("shared:set", new KeyOptions { MergeStrategy = OrSetMergeStrategy.Instance });
await db.PutBytesAsync("shared:set", OrSetValue.Empty.Add("alice", "node-a:1").ToByteArray());
await db.MergeBytesAsync("shared:set", OrSetValue.Empty.Add("bob", "node-b:1").ToByteArray());

var mergedBytes = await db.GetBytesAsync("shared:set");
if (mergedBytes is not null)
{
    var merged = OrSetValue.FromByteArray(mergedBytes);
    Console.WriteLine(string.Join(",", merged.Elements)); // alice,bob
}
```
