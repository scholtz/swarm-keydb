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
| `SWARM_KEYDB_ENCRYPTION_ENABLED` | `false` | Enables AES-256-GCM encryption. |
| `SWARM_KEYDB_ENCRYPTION_ALGORITHM` | `AesGcm256` | Encryption algorithm. |
| `SWARM_KEYDB_ENCRYPTION_KEY` | unset | 32-byte hex key for encryption. |
| `SWARM_KEYDB_ENCRYPTION_ETH_KEY` | unset | Ethereum private key used to derive the encryption key. |
| `SWARM_KEYDB_LOG_LEVEL` | `Information` | Minimum console log level. |

## Bee client integration

| Variable | Default | Purpose |
| --- | --- | --- |
| `BEE_URL` | `http://localhost:1633/` | Bee API base URL used by the Bee-backed store. |
| `BEE_POSTAGE_BATCH_ID` | required for `bee` backend | Postage batch id used for uploads. |

## Deployment defaults

The checked-in Docker Compose and Kubernetes manifests default to:

- `SWARM_KEYDB_BACKEND=bee`
- `BEE_MAINNET=false`
- a Sepolia RPC endpoint placeholder that you must replace