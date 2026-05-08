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
| `AUTHADDR 0x...` | Ethereum address | `OK` | invalid address |
| `QUIT` | none | `OK` and close | wrong arity |

## Stable protocol-visible errors

| Error | Meaning |
| --- | --- |
| `ERR wrong number of arguments for '<COMMAND>' command` | Command arity mismatch. |
| `ERR invalid expire seconds` | `EXPIRE`/`SETEX` seconds must be `> 0`. |
| `ERR invalid expire milliseconds` | `PEXPIRE`/`PSETEX` milliseconds must be `> 0`. |
| `ERR invalid range bounds` | Range start key sorts after end key. |
| `ERR Access denied: address ... does not have ... permission.` | ACL denied the operation. |

## C# `SwarmKeyDbClient` public methods

| Method | Parameters | Returns | Example |
| --- | --- | --- | --- |
| `PutBytesAsync` | `key`, `ReadOnlyMemory<byte>` | `Task` | `await db.PutBytesAsync("k", bytes);` |
| `PutAsync` | `key`, bytes | `Task` | `await db.PutAsync("k", bytes);` |
| `PutBytesWithStrategyAsync` | key, bytes, merge strategy | `Task` | `await db.PutBytesWithStrategyAsync("k", bytes, OrSetMergeStrategy.Instance);` |
| `MergeBytesAsync` | key, incoming bytes | `Task` | `await db.MergeBytesAsync("k", delta);` |
| `SetKeyOptionsAsync` | key, key options | `Task` | `await db.SetKeyOptionsAsync("k", new KeyOptions());` |
| `GetBytesAsync` | key | `Task<byte[]?>` | `var bytes = await db.GetBytesAsync("k");` |
| `GetAsync` | key | `Task<byte[]?>` | `var bytes = await db.GetAsync("k");` |
| `PutStringAsync` | key, string | `Task` | `await db.PutStringAsync("k", "v");` |
| `MergeStringAsync` | key, string | `Task` | `await db.MergeStringAsync("k", "delta");` |
| `GetStringAsync` | key | `Task<string?>` | `var v = await db.GetStringAsync("k");` |
| `PutJsonAsync<T>` | key, value | `Task` | `await db.PutJsonAsync("profile", new { name = "Ada" });` |
| `GetJsonAsync<T>` | key | `Task<T?>` | `var p = await db.GetJsonAsync<Profile>("profile");` |
| `DeleteAsync` | key | `Task<bool>` | `var deleted = await db.DeleteAsync("k");` |
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
