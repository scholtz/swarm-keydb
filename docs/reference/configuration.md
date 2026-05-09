# Configuration Reference

## SwarmKeyDb server

| Variable | Default | Purpose |
| --- | --- | --- |
| `SWARM_KEYDB_BIND` | `0.0.0.0` | Bind address for the Redis-compatible TCP server. |
| `SWARM_KEYDB_PORT` | `6379` | TCP port exposed by the Redis-compatible server. |
| `SWARM_KEYDB_DATA_DIR` | platform-dependent | Local data directory for index and object storage. |
| `BACKEND` | unset | Preferred backend selector (`swarm`, `ipfs`, `hybrid`); overrides `SWARM_KEYDB_BACKEND` when set. |
| `SWARM_KEYDB_BACKEND` | `local` | Legacy backend selector (`local`, `bee`, `swarm`, `ipfs`, `hybrid`). |
| `IPFS_API_URL` | `http://localhost:5001/` | IPFS Kubo HTTP API endpoint used for `ipfs` and `hybrid` backends. |
| `IPFS_PIN_ON_WRITE` | `true` | Pins IPFS objects on write to avoid GC removal. |
| `SWARM_KEYDB_CACHE_ENABLED` | `true` | Enables the read-through cache. |
| `SWARM_KEYDB_CACHE_MAX_ENTRIES` | `1000` | Maximum number of cached entries. |
| `SWARM_KEYDB_CACHE_DEFAULT_TTL_SECONDS` | unset | Optional upper bound for cache entry lifetime. |
| `SWARM_KEYDB_OFFLINE_MODE` | `never` | Offline-first mode (`never`, `auto`, `always`). |
| `SWARM_KEYDB_OFFLINE_JOURNAL` | `memory` | Offline journal backend (`memory`, `sqlite`). |
| `SWARM_KEYDB_OFFLINE_SYNC_INTERVAL_MS` | `5000` | Replay polling interval used by `OfflineSyncService`. |
| `SWARM_KEYDB_COMPRESSION_ENABLED` | `false` | Enables transparent compression. |
| `SWARM_KEYDB_COMPRESSION_ALGORITHM` | `GZip` | Compression algorithm. |
| `SWARM_KEYDB_COMPRESSION_MIN_SIZE_BYTES` | `64` | Minimum payload size for compression. |
| `SWARM_KEYDB_INTEGRITY_ENABLED` | `true` | Enables SHA-256 integrity verification envelopes for stored values. |
| `SWARM_KEYDB_ENCRYPTION_ENABLED` | `false` | Enables AES-256-GCM encryption. |
| `SWARM_KEYDB_ENCRYPTION_ALGORITHM` | `AesGcm256` | Encryption algorithm. |
| `SWARM_KEYDB_ENCRYPTION_KEY` | unset | 32-byte hex key for encryption. |
| `SWARM_KEYDB_ENCRYPTION_ETH_KEY` | unset | Ethereum private key used to derive the encryption key. |
| `SWARM_KEYDB_PRIVACY_MODE` | `None` | Key query privacy mode (`None`, `ObliviousHashing`, `FullPSI`). |
| `SWARM_KEYDB_PRIVACY_KEY` | unset | 32-byte hex key used to derive HMAC key tokens in privacy modes. |
| `SWARM_KEYDB_DID_MODE` | `None` | DID authentication mode (`None`, `EthrDid`). |
| `SWARM_KEYDB_DID_RPC_URL` | unset | Ethereum JSON-RPC URL for on-chain `did:ethr` controller resolution. |
| `SWARM_KEYDB_DID_METHOD` | `ethr` | DID method string (reserved for future extension). |
| `SWARM_KEYDB_ASYNC_ENABLED` | `true` | Enables queued async write processing for high-throughput workloads. |
| `SWARM_KEYDB_MAX_CONCURRENT_WRITES` | `4` | Maximum number of queued write operations processed in parallel. |
| `SWARM_KEYDB_WRITE_BATCH_SIZE` | `64` | Maximum number of queued writes drained per batch. |
| `SWARM_KEYDB_BATCH_FLUSH_INTERVAL_MS` | `100` | Time window used to coalesce queued writes into a batch. |
| `SWARM_KEYDB_SHARDING_ENABLED` | `false` | Enables shard-aware routing with consistent hashing. |
| `SWARM_KEYDB_SHARDING_SHARD_COUNT` | derived from configured nodes (min `1`, max `64`) | Number of hash buckets used for key routing. |
| `SWARM_KEYDB_SHARDING_VIRTUAL_NODES` | `128` | Virtual nodes per shard in the consistent hash ring. |
| `SWARM_KEYDB_SHARDING_NODES` | unset | JSON array of shard nodes (`name`, optional `beeUrl`, optional `postageBatchId`, optional `dataDir`). |

`SWARM_KEYDB_SHARDING_SHARD_COUNT` defaults to the configured shard node count when not explicitly set.
| `SWARM_KEYDB_LOG_LEVEL` | `Information` | Minimum console log level. |
| `LOG_LEVEL` | `Information` | Preferred log level override (`Debug`, `Information`, `Warning`, `Error`). |
| `JSON_LOGS` | auto (`true` outside Development) | Forces JSON (`true`) or simple console (`false`) formatting. |
| `METRICS_ENABLED` | `true` | Enables `/metrics` endpoint. |
| `METRICS_PORT` | `9090` | HTTP port used for Prometheus metrics exposure. |
| `DASHBOARD_ENABLED` | `true` | Enables `/dashboard`, `/health`, `/ready`, and `/logs` endpoints. |
| `DASHBOARD_PORT` | `8080` | HTTP port used by dashboard and health endpoints. |
| `SWARM_KEYDB_CROSS_CHAIN_ENABLED` | `false` | Enables cross-chain replication and reconciliation. |
| `SWARM_KEYDB_CROSS_CHAIN_DEFAULT_CHAIN_IDS` | unset | Comma-separated or JSON array of chain ids replicated by default. |
| `SWARM_KEYDB_CROSS_CHAIN_CHAINS` | unset | JSON array of chain definitions (`chainId`, `name`, optional `rpcUrl`, optional `bridgeContractAddress`). |
| `SWARM_KEYDB_CROSS_CHAIN_MAX_RETRIES` | `5` | Maximum retry attempts before a chain transitions to `failed`. |
| `SWARM_KEYDB_CROSS_CHAIN_RETRY_BASE_SECONDS` | `5` | Base delay used for exponential retry backoff. |
| `SWARM_KEYDB_CROSS_CHAIN_RECONCILE_SECONDS` | `5` | Background reconciliation interval. |

## Bee client integration

| Variable | Default | Purpose |
| --- | --- | --- |
| `BEE_URL` | `http://localhost:1633/` | Bee API base URL used by the Bee-backed store. |
| `BEE_POSTAGE_BATCH_ID` | required for `bee` backend | Postage batch id used for uploads. |

When sharding is enabled and a shard node omits `postageBatchId`, the global `BEE_POSTAGE_BATCH_ID` fallback is used.

## IPFS integration

| Variable | Default | Purpose |
| --- | --- | --- |
| `IPFS_API_URL` | `http://localhost:5001/` | IPFS API endpoint (`api/v0/*`) for add/cat/version operations. |
| `IPFS_PIN_ON_WRITE` | `true` | Whether writes call IPFS add with `pin=true`. |

## CLI (`skdb`)

The CLI stores its persisted settings in `~/.swarmkeydb/config.json`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `SWARMKEYDB_BEE_URL` | `http://localhost:1633/` | Bee API base URL override for CLI commands. |
| `SWARMKEYDB_IPFS_API_URL` | `http://localhost:5001/` | IPFS API base URL override for CLI commands. |
| `SWARMKEYDB_BACKEND` | `swarm` | CLI backend mode (`swarm`, `ipfs`, `hybrid`). |
| `SWARMKEYDB_BATCH_ID` | unset | Postage batch id override for CLI commands. |
| `SWARMKEYDB_OUTPUT` | `plain` | CLI output format override (`plain`, `json`, `table`). |

`skdb put` and `skdb delete` accept `--chains 1,137`, and sync state for `skdb sync status|force --key <key>` is stored in `~/.swarmkeydb/crosschain-sync.json`.

## Deployment defaults

The checked-in Docker Compose and Kubernetes manifests default to:

- `SWARM_KEYDB_BACKEND=bee`
- `BEE_MAINNET=false`
- a Sepolia RPC endpoint placeholder that you must replace

## `appsettings.json` example

The server also reads `src/SwarmKeyDb.Server/appsettings.json`. Cross-chain entries follow this shape:

```json
{
  "CrossChain": {
    "Enabled": true,
    "DefaultChainIds": [1, 137],
    "MaxRetryAttempts": 5,
    "RetryBaseDelaySeconds": 5,
    "ReconcileIntervalSeconds": 5,
    "Chains": [
      {
        "ChainId": 1,
        "Name": "Ethereum",
        "RpcUrl": "https://ethereum.example.invalid",
        "BridgeContractAddress": "0x0000000000000000000000000000000000000000"
      },
      {
        "ChainId": 137,
        "Name": "Polygon",
        "RpcUrl": "https://polygon.example.invalid",
        "BridgeContractAddress": "0x0000000000000000000000000000000000000000"
      }
    ]
  }
}
```
