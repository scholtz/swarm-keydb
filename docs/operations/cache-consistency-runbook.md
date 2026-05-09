# Cache Consistency Runbook

Use this runbook to monitor and alert on multi-node cache convergence health.

## Sync and resync configuration

| Variable | Meaning |
| --- | --- |
| `SWARM_KEYDB_SYNC_PEERS` | Other SwarmKeyDb Redis endpoints used for cache invalidation + version reconciliation. |
| `SWARM_KEYDB_SYNC_INTERVAL_SEC` | Anti-entropy interval. Lower values reduce convergence delay but increase background load. |
| `SWARM_KEYDB_SYNC_CHANNEL` | Redis pub/sub channel name for cache invalidation events. |
| `SWARM_KEYDB_SYNC_NODE_ID` | Stable node identity used in cache sync events and reconciliation logs. |
| `SWARM_KEYDB_RESYNC_MODE` | Startup/default resync strategy (`auto`, `partial`, `full`). |
| `SWARM_KEYDB_RESYNC_MAX_VERSION_GAP` | Gap threshold used by `auto` mode to switch from partial to full resync. |
| `SWARM_KEYDB_RESYNC_FULL_BATCH_SIZE` | Number of keys replayed per full-resync batch. |
| `SWARM_KEYDB_RESYNC_TIMEOUT_SECONDS` | Timeout for each resync operation. |

## Consistency telemetry (`/metrics`)

| Metric | Type | Interpretation |
| --- | --- | --- |
| `swarmkeydb_cache_drift_total` | counter | Number of drifted keys reconciled by anti-entropy cycles. |
| `swarmkeydb_sync_lag_keys` | gauge | Current pending reconciliation backlog (key-level sync lag). |
| `swarmkeydb_resync_partial_total` | counter | Completed partial resync operations. |
| `swarmkeydb_resync_full_total` | counter | Completed full resync operations. |
| `swarmkeydb_resync_duration_seconds` | gauge | Duration of the last completed resync run. |
| `swarmkeydb_resync_keys_replayed_total` | counter | Cumulative keys replayed by resync operations. |
| `swarmkeydb_cache_verification_fail_total` | counter | Cache reads that failed consistency verification. |
| `swarmkeydb_cache_eviction_by_verification_total` | counter | Cache entries evicted due to verification failures. |

## Suggested alerts

- **Sync lag is growing:** alert if `swarmkeydb_sync_lag_keys > 0` for 10m.
- **Drift spike:** alert if `increase(swarmkeydb_cache_drift_total[5m]) > 100`.
- **Frequent full rebuilds:** alert if `increase(swarmkeydb_resync_full_total[15m]) >= 1`.
- **Verification failures:** alert if `increase(swarmkeydb_cache_verification_fail_total[5m]) > 0`.

Start with warning severity, then tighten thresholds after observing your steady-state traffic.
