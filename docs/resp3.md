# RESP3 Protocol Support

SwarmKeyDb implements RESP3 (REdis Serialization Protocol version 3) introduced in Redis 6.0, enabling full compatibility with all modern Redis client libraries.

## Overview

RESP3 is negotiated per-connection using the `HELLO` command. Connections default to RESP2 on open; sending `HELLO 3` upgrades the connection. You can always downgrade back with `HELLO 2` or fully reset the connection state with `RESET`.

## `HELLO` Command

```
HELLO [protover [AUTH username password] [SETNAME clientname]]
```

| Form | Behaviour |
|------|-----------|
| `HELLO` | Returns current server info in the current protocol encoding. No version change. |
| `HELLO 2` | Acknowledges RESP2 (no-op if already on RESP2). Returns server info as a flat array. |
| `HELLO 3` | Upgrades the connection to RESP3. Returns server info as a RESP3 Map. |
| `HELLO 3 AUTH default password` | Upgrades to RESP3 and authenticates in one round-trip. |
| `HELLO 3 SETNAME myapp` | Upgrades to RESP3 and sets the connection name. |

### Response shape

The response is a **Map** (RESP3 `%7\r\n…`) containing the following keys:

| Key | Example value |
|-----|---------------|
| `server` | `swarmkeydb` |
| `version` | `7.0.0` |
| `proto` | `3` (integer) |
| `id` | `1` (connection ID) |
| `mode` | `standalone` |
| `role` | `master` |
| `modules` | `[]` (empty array) |

In RESP2 mode the same keys are returned as a flat (alternating key/value) array, exactly matching Redis 7.x behaviour.

### Error codes

| Error | Meaning |
|-------|---------|
| `-NOPROTO unsupported protocol version` | Requested protocol version is not 2 or 3. |
| `-WRONGPASS invalid username-password pair or user is disabled.` | AUTH subargument password mismatch. |
| `-ERR Command not allowed inside a transaction` | HELLO called inside a MULTI block. |

---

## RESP3 Data Types

When a connection is in RESP3 mode (`HELLO 3`) the server uses richer encodings. When the connection stays in RESP2 mode each type degrades gracefully so existing clients see no change.

| RESP3 type | Wire prefix | RESP2 degradation | Notes |
|-----------|-------------|-------------------|-------|
| **Null** | `_\r\n` | `$-1\r\n` | Replaces null bulk strings |
| **Map** | `%<n>\r\n` | `*<2n>\r\n` | Used by `HELLO`, `HGETALL`, `CONFIG GET` |
| **Set** | `~<n>\r\n` | `*<n>\r\n` | Used by `SMEMBERS` etc. |
| **Double** | `,<value>\r\n` | `$<len>\r\n<value>\r\n` | Used by `ZSCORE`, `ZINCRBY` |
| **Boolean** | `#t\r\n` / `#f\r\n` | `:1\r\n` / `:0\r\n` | True/false replies |
| **BigNumber** | `(<digits>\r\n` | `$<len>\r\n<digits>\r\n` | Very large integers |
| **VerbatimString** | `=<len>\r\n<enc>:<data>\r\n` | `$<datalen>\r\n<data>\r\n` | Human-readable text |
| **BlobError** | `!<len>\r\n<msg>\r\n` | `-<msg>\r\n` | Rich error with length prefix |
| **Push** | `><n>\r\n…` | `*<n>\r\n…` | Out-of-band pub/sub and invalidation |

---

## `RESET` Command

```
RESET
```

Resets the connection to its **factory state**:

- Downgrade protocol to RESP2.
- Unsubscribe from all pub/sub channels and patterns.
- Clear any ongoing MULTI transaction.
- Remove CLIENT TRACKING registration.
- Clear authenticated address / DID context.

Returns `+RESET\r\n`.

---

## Client Tracking

```
CLIENT TRACKING ON|OFF [REDIRECT client-id] [BCAST] [PREFIX prefix] [NOLOOP]
```

SwarmKeyDb supports basic server-assisted client-side caching via `CLIENT TRACKING`.

### Activation

```
HELLO 3
CLIENT TRACKING ON
```

Once enabled, the server sends **push invalidation** messages on the same connection whenever a tracked key is modified by another connection. The invalidation frame has the form:

```
>2\r\n
$10\r\ninvalidate\r\n
*1\r\n
$<keylen>\r\n<key>\r\n
```

### Invalidation triggers

The following write commands trigger invalidation pushes:

`SET`, `SETEX`, `PSETEX`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PERSIST`, `DEL`, `MDEL`, `MSET`, `MSETNX`, `XADD`, `XTRIM`, `XGROUP`, `XREADGROUP`, `XACK`, `XCLAIM`, `XAUTOCLAIM`

### NOLOOP behaviour

Invalidations are **not** delivered to the connection that performed the write (implicit NOLOOP), preventing self-invalidation without extra configuration.

### Delivery timing

Push invalidation frames are flushed to the client **after the reply to each command** on the tracking connection. This ensures they arrive between regular replies without disrupting the command/reply stream.

### Deactivation

```
CLIENT TRACKING OFF
```

Returns `+OK\r\n`. Future writes will no longer produce push frames on this connection.

---

## Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `swarmkeydb_resp3_connections_total` | counter | Total connections that have ever negotiated RESP3 via `HELLO 3`. |
| `swarmkeydb_resp3_active_connections` | gauge | Current connections running in RESP3 mode. |
| `swarmkeydb_client_tracking_keys_total` | gauge | Current connections with `CLIENT TRACKING ON`. |
| `swarmkeydb_ws_resp3_connections_total` | counter | Total WebSocket connections that negotiated RESP3. |
| `swarmkeydb_ws_client_tracking_connections` | gauge | Current WebSocket connections with client tracking enabled. |
| `swarmkeydb_http_resp3_requests_total` | counter | Total HTTP requests using `Accept: application/json; resp=3`. |

---

## WebSocket RESP3

The WebSocket gateway also supports RESP3 negotiation. Send either:

- JSON array: `["HELLO","3"]`
- Raw RESP frame over WebSocket: `*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n`

In JSON mode, RESP3 push invalidations are emitted as:

```json
{"type":"push","data":["invalidate",["my-key"]]}
```

`RESET` returns `+RESET` in RESP mode and resets protocol/tracking state back to defaults.

---

## HTTP REST RESP3 negotiation

The HTTP gateway supports RESP3-style JSON with:

`Accept: application/json; resp=3`

Examples:

- `EXISTS` → boolean JSON
- `CONFIG GET` → JSON object map
- `ZSCORE` → JSON number

---

## Client Library Examples

### redis-py ≥ 4.x (RESP3 by default)

```python
import redis

# redis-py 4.x negotiates RESP3 automatically
r = redis.Redis(host="localhost", port=6379, protocol=3)
r.ping()           # → True
r.set("k", "v")   # → True
r.get("k")         # → b"v"
```

### StackExchange.Redis 2.6+

```csharp
using StackExchange.Redis;

var options = ConfigurationOptions.Parse("localhost:6379");
options.ProtocolVersion = RedisProtocolVersion.Resp3;
var mux = await ConnectionMultiplexer.ConnectAsync(options);
var db = mux.GetDatabase();
await db.StringSetAsync("hello", "world");
var val = await db.StringGetAsync("hello"); // → "world"
```

### redis-cli with RESP3

```bash
redis-cli -3 PING
# PONG

redis-cli -3 HELLO
# %7
# "server"
# "swarmkeydb"
# ...
```

---

## Migration Guide (RESP2 → RESP3)

1. **No code changes required** for existing RESP2 clients — RESP2 is the default and is never changed unless the client sends `HELLO 3`.
2. To opt in, configure your client library to use RESP3 (usually `protocol=3` or `ProtocolVersion.Resp3`).
3. If your application inspects raw replies (e.g. parses `*-1\r\n` as nil), update the nil-check to also handle `_\r\n`.
4. `HGETALL` now returns a native map in RESP3, so client libraries that previously reconstructed the dict from a flat list will receive a proper map type.

---

## Limitations (current release)

- `CLIENT TRACKING REDIRECT` (redirecting invalidations to a different connection) is not yet implemented.
- Cluster-mode shard tracking is out of scope.
