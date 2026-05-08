# Configuration Reference

## SwarmKeyDb server

| Variable | Default | Purpose |
| --- | --- | --- |
| `SWARM_KEYDB_BIND` | `0.0.0.0` | Bind address for the Redis-compatible TCP server. |
| `SWARM_KEYDB_PORT` | `6379` | TCP port exposed by the Redis-compatible server. |
| `SWARM_KEYDB_DATA_DIR` | platform-dependent | Local data directory for index and object storage. |
| `SWARM_KEYDB_BACKEND` | `local` | Storage backend selection: `local` or `bee`. |
| `SWARM_KEYDB_CACHE_ENABLED` | `true` | Enables the read-through cache. |
| `SWARM_KEYDB_CACHE_MAX_ENTRIES` | `1000` | Maximum number of cached entries. |
| `SWARM_KEYDB_CACHE_DEFAULT_TTL_SECONDS` | unset | Optional upper bound for cache entry lifetime. |
| `SWARM_KEYDB_COMPRESSION_ENABLED` | `false` | Enables transparent compression. |
| `SWARM_KEYDB_COMPRESSION_ALGORITHM` | `GZip` | Compression algorithm. |
| `SWARM_KEYDB_COMPRESSION_MIN_SIZE_BYTES` | `64` | Minimum payload size for compression. |
| `SWARM_KEYDB_INTEGRITY_ENABLED` | `true` | Enables SHA-256 integrity verification envelopes for stored values. |
| `SWARM_KEYDB_ENCRYPTION_ENABLED` | `false` | Enables AES-256-GCM encryption. |
| `SWARM_KEYDB_ENCRYPTION_ALGORITHM` | `AesGcm256` | Encryption algorithm. |
| `SWARM_KEYDB_ENCRYPTION_KEY` | unset | 32-byte hex key for encryption. |
| `SWARM_KEYDB_ENCRYPTION_ETH_KEY` | unset | Ethereum private key used to derive the encryption key. |
| `SWARM_KEYDB_ASYNC_ENABLED` | `true` | Enables queued async write processing for high-throughput workloads. |
| `SWARM_KEYDB_MAX_CONCURRENT_WRITES` | `4` | Maximum number of queued write operations processed in parallel. |
| `SWARM_KEYDB_WRITE_BATCH_SIZE` | `64` | Maximum number of queued writes drained per batch. |
| `SWARM_KEYDB_BATCH_FLUSH_INTERVAL_MS` | `100` | Time window used to coalesce queued writes into a batch. |
| `SWARM_KEYDB_LOG_LEVEL` | `Information` | Minimum console log level. |
| `LOG_LEVEL` | `Information` | Preferred log level override (`Debug`, `Information`, `Warning`, `Error`). |
| `JSON_LOGS` | auto (`true` outside Development) | Forces JSON (`true`) or simple console (`false`) formatting. |
| `METRICS_ENABLED` | `true` | Enables `/metrics` endpoint. |
| `METRICS_PORT` | `9090` | HTTP port used for Prometheus metrics exposure. |
| `DASHBOARD_ENABLED` | `true` | Enables `/dashboard`, `/health`, `/ready`, and `/logs` endpoints. |
| `DASHBOARD_PORT` | `8080` | HTTP port used by dashboard and health endpoints. |

## Bee client integration

| Variable | Default | Purpose |
| --- | --- | --- |
| `BEE_URL` | `http://localhost:1633/` | Bee API base URL used by the Bee-backed store. |
| `BEE_POSTAGE_BATCH_ID` | required for `bee` backend | Postage batch id used for uploads. |

## CLI (`skdb`)

The CLI stores its persisted settings in `~/.swarmkeydb/config.json`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `SWARMKEYDB_BEE_URL` | `http://localhost:1633/` | Bee API base URL override for CLI commands. |
| `SWARMKEYDB_BATCH_ID` | unset | Postage batch id override for CLI commands. |
| `SWARMKEYDB_OUTPUT` | `plain` | CLI output format override (`plain`, `json`, `table`). |

## Deployment defaults

The checked-in Docker Compose and Kubernetes manifests default to:

- `SWARM_KEYDB_BACKEND=bee`
- `BEE_MAINNET=false`
- a Sepolia RPC endpoint placeholder that you must replace
