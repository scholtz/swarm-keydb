# Offline-First Support

SwarmKeyDb now supports an offline-first execution path for Bee/Swarm-backed deployments.

## What ships in this iteration

- `OfflineCapableKeyValueStore` queues `put` and `delete` operations when the backend is unreachable.
- `OfflineSyncService` replays queued operations in order when connectivity returns.
- `IConnectivityProbe` plus `HttpHealthConnectivityProbe` poll the Bee `/health` endpoint.
- `InMemoryOfflineJournal` and `SqliteOfflineJournal` support ephemeral or restart-safe buffering.
- `SwarmKeyDbOptions.OnConflict` allows applications to override the default local-wins replay when queued data conflicts with remote state.
- `/health`, `/ready`, `/metrics`, and `/dashboard` expose offline queue depth and last sync status.

## Architecture

```text
App / Redis client
        |
        v
OfflineCapableKeyValueStore
  |  local cache (serves reads during outages)
  |  offline journal (memory/sqlite)
  v
Crdt / compression / encryption decorators
  v
Swarm / Bee backend

OfflineSyncService ---> IConnectivityProbe (/health)
        |
        +--> replays queued operations in sequence order
        +--> invokes OnConflict(local, remote) when both sides changed
```

## C# usage

```csharp
using SwarmKeyDb;

var remoteStore = new SwarmKeyValueStore(new BeeSwarmClient(beeUrl, postageBatchId), new FileKeyIndex("data/index.json"));
var offlineStore = new OfflineCapableKeyValueStore(
    remoteStore,
    new SqliteOfflineJournal("data/offline-journal.sqlite"),
    new HttpHealthConnectivityProbe(beeUrl),
    new SwarmKeyDbOptions
    {
        OfflineMode = OfflineMode.Auto,
        OfflineJournal = OfflineJournalType.Sqlite,
        OfflineSyncIntervalMs = 2_000,
        OnConflict = context => context.LocalValue
    });
var sync = new OfflineSyncService(offlineStore, TimeSpan.FromSeconds(2));
await sync.StartAsync();

var write = await offlineStore.PutWithResultAsync("user:profile", "{\"name\":\"Ada\"}"u8.ToArray());
Console.WriteLine($"queued={write.Queued} depth={write.QueueDepth}");

var read = await offlineStore.GetWithResultAsync("user:profile");
Console.WriteLine($"fromCache={read.FromCache} cachedAt={read.CachedAt}");
```

## Conflict resolution

During replay, queued writes are compared with the current remote value:

- if there is no remote value, the queued value is written as-is
- if local and remote values match, replay proceeds normally
- if both differ, `SwarmKeyDbOptions.OnConflict` is invoked with both payloads
- if no callback is configured, the queued local value is used

Because replay still writes through the CRDT-enabled store pipeline, existing merge envelopes and key-specific merge strategies continue to apply.

## Server configuration

| Variable | Values | Default | Purpose |
| --- | --- | --- | --- |
| `SWARM_KEYDB_OFFLINE_MODE` | `never`, `auto`, `always` | `never` | Enables offline-first behavior. |
| `SWARM_KEYDB_OFFLINE_JOURNAL` | `memory`, `sqlite` | `memory` | Selects the local operation journal implementation. |
| `SWARM_KEYDB_OFFLINE_SYNC_INTERVAL_MS` | integer | `5000` | Poll interval for background replay attempts. |

When `sqlite` mode is selected, the server stores the journal in `<data-dir>/offline-journal.sqlite`.

## Monitoring

- `GET /health` and `GET /ready` include `offline_queue_depth` and `last_successful_sync_utc`
- `GET /metrics` exposes `swarmkeydb_offline_queue_depth` and `swarmkeydb_offline_last_sync_unix_time`
- `GET /dashboard` shows the offline queue depth and last successful sync timestamp

## Example flow

1. Start the server with `SWARM_KEYDB_OFFLINE_MODE=auto`.
2. Stop or partition Bee.
3. Continue issuing `SET`/`DEL` commands; the server journals them locally.
4. Restore Bee connectivity.
5. Wait for `OfflineSyncService` to drain the queue and verify `/health` shows `offline_queue_depth: 0`.
