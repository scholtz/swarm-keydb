# Performance Tuning

## Index Architecture

SwarmKeyDb uses a pluggable index layer (the `IKeyIndex` interface) that sits between the value store and the Swarm network. The index maps every key to a Swarm content-address reference so that individual key lookups avoid a full network scan.

Two built-in index strategies are provided:

| Strategy | Class | Lookup | Range / Prefix scan | Iteration |
|---|---|---|---|---|
| `IndexStrategy.Dictionary` | `InMemoryKeyIndex` | O(1) amortised | O(n log n) — sorts all keys per query | O(n) |
| `IndexStrategy.BTree` | `BTreeKeyIndex` | O(log n) | O(s + k) — skips s keys before the range start, then visits k results | O(n) already-sorted |

`k` is the number of keys in the result set.  `n` is the total number of keys.  `s` is the number of keys that precede the lower bound of the range.

### BTree index internals

`BTreeKeyIndex` is backed by `SortedDictionary<string, KeyIndexEntry>`, which is a Red-Black tree in the .NET runtime. Because the tree stores keys in sorted order at all times, the index can:

- **Terminate early** in range scans as soon as the first key past the upper bound is encountered, visiting only `s + k` entries instead of all `n`.
- Return `ListKeysAsync` results that are **already sorted**, avoiding the `.Order()` call required by `InMemoryKeyIndex`.

> **Note on lower-bound seek:** `SortedDictionary` does not expose a cursor-based seek API, so `GetKeysInRangeAsync` iterates from the smallest key until the lower bound is reached (O(s) skip). In the common case where queries start near the beginning of the key space (e.g., time-ordered keys with a recent cursor), `s ≈ 0` and the scan is effectively O(k). A future implementation backed by a custom B-tree or `SortedList` with binary-search-based skip could reduce this to O(log n + k).

### Prefix scans

`SwarmKeyValueStore.GetKeysWithPrefixAsync` converts the prefix to an exclusive upper bound (by incrementing the last character) and then delegates to `IKeyIndex.GetKeysInRangeAsync`. For example, prefix `"user:alice:"` maps to the range `["user:alice:", "user:alice;")` — `;` is the character immediately after `:` in Unicode. This means only keys that actually share the prefix are visited.

### Range scans

`SwarmKeyValueStore.GetKeyRangeAsync` now delegates filtering to `IKeyIndex.GetKeysInRangeAsync` rather than fetching all keys and filtering in memory. Implementations that override `GetKeysInRangeAsync` (like `BTreeKeyIndex`) can therefore deliver O(k) results without materialising the full key list.

---

## Selecting an index strategy

Use `BTreeKeyIndex` when your workload includes:

- Frequent range queries (e.g., time-series keys, paginated listings).
- Prefix scans over large namespaces.
- Many thousands of keys where the O(n log n) sort in `InMemoryKeyIndex.ListKeysAsync` becomes measurable.

Use `InMemoryKeyIndex` (the default) when:

- Your key set is small (fewer than a few hundred entries).
- Workload is predominantly single-key lookups and you want the O(1) hash-map benefit.

### Configuration example

```csharp
// Use the BTree index when constructing the store directly:
var index = new BTreeKeyIndex();
var store = new SwarmKeyValueStore(swarmClient, index);
```

For the server binary, pass a `BTreeKeyIndex` to `AddSwarmKeyDbStore`:

```csharp
services.AddSwarmKeyDbStore(swarmClient, new BTreeKeyIndex());
```

---

## Rebuilding the index

Both `IKeyIndex` implementations expose `RebuildIndexAsync()`. For `BTreeKeyIndex` and `InMemoryKeyIndex` this purges expired (TTL-based) entries from the in-memory tree. For file-backed or Swarm-backed implementations it should reload durable state.

```csharp
await index.RebuildIndexAsync();
```

Call this after an unexpected restart or if you suspect index corruption.

---

## Expected complexity summary

| Operation | `InMemoryKeyIndex` | `BTreeKeyIndex` |
|---|---|---|
| `GetReferenceAsync` | O(1) | O(log n) |
| `SetReferenceAsync` | O(1) | O(log n) |
| `RemoveAsync` | O(1) | O(log n) |
| `ListKeysAsync` | O(n log n) | O(n) |
| `GetKeysInRangeAsync` | O(n log n) | O(s + k) †  |
| `GetKeysWithPrefixAsync` | O(n) | O(s + k) † |

† `s` = number of keys before the lower bound; `k` = result count.
In the best case (`s ≈ 0`) this is O(k).
