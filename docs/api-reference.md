# API Reference

This reference covers the public Redis protocol surface and `SwarmKeyDbClient` methods.

## Redis commands

| Command | Parameters | Result | Common errors |
| --- | --- | --- | --- |
| `PING` | none | `PONG` | `ERR wrong number of arguments for 'PING' command` |
| `SET key value [EX seconds\|PX milliseconds\|EXAT unixSeconds]` | key, value, optional TTL | `OK` | invalid TTL, wrong arity |
| `SETEX key seconds value` | key, positive seconds, value | `OK` | `ERR invalid expire seconds` |
| `PSETEX key milliseconds value` | key, positive milliseconds, value | `OK` | `ERR invalid expire milliseconds` |
| `GET key` | key | bulk string or null | access denied |
| `MGET key [key...]` | keys | array of values/nulls | wrong arity, access denied |
| `MSET key value [key value...]` | key/value pairs | `OK` | wrong arity |
| `MSETNX key value [key value...]` | key/value pairs | integer `1` or `0` | wrong arity |
| `DEL key [key...]` / `MDEL key [key...]` | keys | deleted count | wrong arity |
| `EXISTS key [key...]` | keys | existing key count | wrong arity |
| `EXPIRE key seconds` / `PEXPIRE key milliseconds` / `EXPIREAT key unixSeconds` | key + TTL value | integer `1` or `0` | invalid TTL |
| `TTL key` / `PTTL key` | key | remaining TTL, `-1`, or `-2` | wrong arity |
| `PERSIST key` | key | integer `1` or `0` | wrong arity |
| `KEYS pattern` | glob pattern | matching keys | wrong arity |
| `SCAN cursor [COUNT n]` | cursor token | keys + next cursor | invalid cursor/count |
| `TYPE key` | key | `string` / `none` | wrong arity |
| `XADD key [MAXLEN [~\|=] count] id field value [field value ...]` | stream key, id token, field/value pairs | generated or explicit stream ID | wrong arity, invalid IDs, out-of-order IDs, wrong type |
| `XTRIM key MAXLEN [~\|=] count` / `XTRIM key MINID [~\|=] id` | stream key + retention strategy | integer count of deleted entries | wrong arity, invalid arguments, wrong type |
| `XRANGE key start end [COUNT n]` | stream key, range bounds | nested array of stream entries | wrong arity, invalid IDs, wrong type |
| `XREVRANGE key end start [COUNT n]` | stream key, reverse range bounds | nested array of stream entries | wrong arity, invalid IDs, wrong type |
| `XLEN key` | stream key | stream entry count (`0` if missing) | wrong arity, wrong type |
| `XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]` | stream keys + start IDs (`$` supported) | stream entries or null array on timeout/no data | wrong arity, invalid IDs, wrong type |
| `XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]` | consumer-group read (`>` or explicit pending IDs) | stream entries or null array on timeout/no data | wrong arity, invalid IDs, wrong type, `NOGROUP` |
| `AUTHADDR 0x...` | Ethereum address | `OK` | invalid address |
| `QUIT` | none | `OK` and close | wrong arity |

## Stable protocol-visible errors

| Error | Meaning |
| --- | --- |
| `ERR wrong number of arguments for '<COMMAND>' command` | Command arity mismatch. |
| `ERR invalid expire seconds` | `EXPIRE`/`SETEX` seconds must be `> 0`. |
| `ERR invalid expire milliseconds` | `PEXPIRE`/`PSETEX` milliseconds must be `> 0`. |
| `ERR invalid range bounds` | Range start key sorts after end key. |
| `ERR Invalid stream ID specified as stream command argument` | Stream ID token is malformed. |
| `ERR The ID specified in XADD is equal or smaller than the target stream top item` | Stream IDs must increase monotonically per key. |
| `ERR The ID specified in XADD must be greater than 0-0` | `0-0` is not a valid new stream entry ID. |
| `WRONGTYPE Operation against a key holding the wrong kind of value` | Stream command used on a non-stream key (or vice versa). |
| `ERR Access denied: address ... does not have ... permission.` | ACL denied the operation. |

## C# `SwarmKeyDbClient` public methods

| Method | Parameters | Returns | Example |
| --- | --- | --- | --- |
| `PutBytesAsync` | `key`, `ReadOnlyMemory<byte>` | `Task` | `await db.PutBytesAsync("k", bytes);` |
| `PutBytesAsync` | `key`, bytes, `chainIds` | `Task` | `await db.PutBytesAsync("k", bytes, new[] { 1, 137 });` |
| `PutAsync` | `key`, bytes | `Task` | `await db.PutAsync("k", bytes);` |
| `PutAsync` | `key`, bytes, `chainIds` | `Task` | `await db.PutAsync("k", bytes, new[] { 1, 137 });` |
| `PutBytesWithStrategyAsync` | key, bytes, merge strategy | `Task` | `await db.PutBytesWithStrategyAsync("k", bytes, OrSetMergeStrategy.Instance);` |
| `MergeBytesAsync` | key, incoming bytes | `Task` | `await db.MergeBytesAsync("k", delta);` |
| `SetKeyOptionsAsync` | key, key options | `Task` | `await db.SetKeyOptionsAsync("k", new KeyOptions());` |
| `GetBytesAsync` | key | `Task<byte[]?>` | `var bytes = await db.GetBytesAsync("k");` |
| `GetAsync` | key | `Task<byte[]?>` | `var bytes = await db.GetAsync("k");` |
| `PutStringAsync` | key, string | `Task` | `await db.PutStringAsync("k", "v");` |
| `PutStringAsync` | key, string, `IEnumerable<ChainId>` | `Task` | `await db.PutStringAsync("k", "v", new[] { ChainId.Ethereum, ChainId.Polygon });` |
| `MergeStringAsync` | key, string | `Task` | `await db.MergeStringAsync("k", "delta");` |
| `GetStringAsync` | key | `Task<string?>` | `var v = await db.GetStringAsync("k");` |
| `PutJsonAsync<T>` | key, value | `Task` | `await db.PutJsonAsync("profile", new { name = "Ada" });` |
| `PutJsonAsync<T>` | key, value, `chainIds` | `Task` | `await db.PutJsonAsync("profile", new { name = "Ada" }, new[] { 1, 137 });` |
| `GetJsonAsync<T>` | key | `Task<T?>` | `var p = await db.GetJsonAsync<Profile>("profile");` |
| `DeleteAsync` | key | `Task<bool>` | `var deleted = await db.DeleteAsync("k");` |
| `DeleteAsync` | key, `chainIds` | `Task<bool>` | `var deleted = await db.DeleteAsync("k", new[] { 1, 137 });` |
| `GetSyncStatusAsync` | key | `Task<CrossChainSyncStatus?>` | `var status = await db.GetSyncStatusAsync("k");` |
| `ForceSyncAsync` | key | `Task<bool>` | `await db.ForceSyncAsync("k");` |
| `BatchGetAsync` | key list | `Task<IReadOnlyList<byte[]?>>` | `var vals = await db.BatchGetAsync(keys);` |
| `BatchPutAsync` | key/value pairs | `Task` | `await db.BatchPutAsync(entries);` |
| `KeysAsync` | none | `Task<IReadOnlyList<string>>` | `var keys = await db.KeysAsync();` |
| `GetKeysWithPrefixAsync` | prefix | `Task<IReadOnlyList<string>>` | `var keys = await db.GetKeysWithPrefixAsync("user:");` |
| `GetKeyRangeAsync` | start, end, options | `Task<IReadOnlyList<RangeScanEntry>>` | `var r = await db.GetKeyRangeAsync("a", "z");` |
| `QueryAsync` | key predicate, optional value predicate | `IAsyncEnumerable<KeyValuePair<string, byte[]>>` | `await foreach (var e in db.QueryAsync(k => k.StartsWith("o:"))) { }` |
| `ScanAsync` | cursor, count | `Task<ScanResult>` | `var page = await db.ScanAsync(null, 100);` |
| `WithNamespace` | prefix | `SwarmKeyDbClient` | `var ns = db.WithNamespace("users:42:");` |
| `DeleteNamespaceAsync` | prefix | `Task<int>` | `var n = await db.DeleteNamespaceAsync("users:42:");` |
| `FlushAsync` | none | `Task` | `await db.FlushAsync();` |
| `FireAndForget(Func<Task>)` | operation, name | `void` | `db.FireAndForget(() => db.PutStringAsync("k","v"));` |
| `FireAndForget(Action)` | operation, name | `void` | `db.FireAndForget(() => Console.WriteLine("done"));` |

## Runnable references

- `examples/RangeScanExample/Program.cs`
- `examples/UserProfileExample/Program.cs`
- `examples/BatchOperationsExample/Program.cs`
- `swarm-keydb-js/examples/*.mjs`
- `swarm-keydb-py/examples/*.py`
- `swarm-keydb-go/examples/*/main.go`

## Monitoring HTTP endpoints

When the dashboard/monitoring server is enabled:

- `GET /sync/{key}` returns per-chain replication status for a key.
- `GET /sync` returns per-chain summary counts used by the dashboard health table.
