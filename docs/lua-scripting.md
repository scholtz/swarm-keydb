# Lua Scripting (`EVAL`, `EVALSHA`, `SCRIPT`)

SwarmKeyDb supports Redis-compatible Lua scripting through the [MoonSharp](https://www.moonsharp.org/)
interpreter (MIT licence). Scripts execute atomically on the server — no other
command can interleave during a single `EVAL` or `EVALSHA` invocation.

---

## Commands

### `EVAL script numkeys [key …] [arg …]`

Executes a Lua script inline.

```
EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 mykey myvalue
```

- `script` — raw Lua source code.
- `numkeys` — number of KEYS arguments that follow (0 or more).
- `KEYS` — 1-based Lua table of key names (`KEYS[1]`, `KEYS[2]`, …).
- `ARGV` — 1-based Lua table of extra arguments (`ARGV[1]`, `ARGV[2]`, …).

### `EVALSHA sha1 numkeys [key …] [arg …]`

Executes a previously cached script by its SHA1 hex digest. Returns
`-NOSCRIPT No matching script. Please use EVAL.` on cache miss.

```
EVALSHA a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 0
```

### `SCRIPT LOAD script`

Stores a script in the node-local cache and returns its SHA1:

```
SCRIPT LOAD "return 1"
```

### `SCRIPT EXISTS sha1 [sha1 …]`

Returns an array of `0`/`1` flags indicating which SHAs are cached:

```
SCRIPT EXISTS a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 0000000000000000000000000000000000000000
```

### `SCRIPT FLUSH [ASYNC|SYNC]`

Clears all cached scripts. The mode flag is accepted but ignored (single-node):

```
SCRIPT FLUSH
```

### `SCRIPT KILL`

Returns `-NOTBUSY No scripts in execution right now.` when no script is active.
Because scripts run on detached tasks in this implementation, mid-flight
termination is not supported; the timeout guard handles runaway scripts.

---

## `redis.call` and `redis.pcall`

Inside a script, use `redis.call(cmd, arg1, …)` to execute any SwarmKeyDb command:

```lua
redis.call("SET", KEYS[1], ARGV[1])
local val = redis.call("GET", KEYS[1])
return val
```

`redis.call` propagates errors as Lua errors (aborting the script).
`redis.pcall` catches errors and returns a table `{err = "message"}` instead:

```lua
local r = redis.pcall("BADCOMMAND")
if r.err then
  return "caught: " .. r.err
end
```

---

## Lua → RESP type mapping

| Lua type | RESP reply |
|----------|-----------|
| number | Integer (truncated to integer) |
| string | Bulk string |
| boolean `true` | Integer `1` |
| boolean `false` / nil | Nil bulk string |
| table `{ok="..."}` | Simple string |
| table `{err="..."}` | Error reply |
| table (array) | Multi-bulk array (recursive) |

---

## Sandbox restrictions

The following Lua globals are **stripped** and unavailable inside scripts:

- `io`, `os`, `package`
- `dofile`, `loadfile`, `require`, `load`
- `rawget`, `rawset`, `collectgarbage`

Accessing any of these from a script returns `nil`.

---

## Timeout guard

Scripts that exceed the configured time limit receive a `BUSY` error and the
command loop returns immediately:

```
-BUSY Script exceeded time limit
```

Configure the limit via `SWARM_KEYDB_SCRIPT_TIMEOUT_MS` (default: `5000` ms).
The background task that runs the script completes on its own thread without
blocking subsequent requests.

---

## Output size limit

Scripts returning more than **10 MiB** of data in a single reply receive:

```
-ERR script output too large
```

---

## Common patterns

### Rate limiter

```lua
-- EVAL "<script>" 1 rate:user:123 1 60
local current = redis.call("INCR", KEYS[1])
if current == 1 then
  redis.call("EXPIRE", KEYS[1], ARGV[2])
end
if tonumber(current) > tonumber(ARGV[1]) then
  return 0   -- rate-limited
end
return 1     -- allowed
```

### Atomic lock acquire (Redlock-style)

```lua
-- EVAL "<script>" 1 lock:resource owner1 10
if redis.call("EXISTS", KEYS[1]) == 0 then
  redis.call("SET", KEYS[1], ARGV[1])
  redis.call("EXPIRE", KEYS[1], ARGV[2])
  return 1   -- lock acquired
end
return 0     -- lock already held
```

### Atomic lock release (safe delete)

```lua
-- EVAL "<script>" 1 lock:resource owner1
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("DEL", KEYS[1])
  return 1   -- released
end
return 0     -- not owner, not released
```

### Counter with expiry reset

```lua
-- EVAL "<script>" 1 counter:daily 3600
redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], ARGV[1])
return redis.call("GET", KEYS[1])
```

---

## Prometheus metrics

| Metric | Type | Description |
|--------|------|-------------|
| `swarmkeydb_script_eval_total` | Counter | Total `EVAL` invocations. |
| `swarmkeydb_script_evalsha_total` | Counter | Total `EVALSHA` invocations. |
| `swarmkeydb_script_error_total` | Counter | Total script errors (runtime + timeout). |
| `swarmkeydb_script_timeout_total` | Counter | Scripts terminated by the timeout guard. |
| `swarmkeydb_script_exec_duration_seconds` | Histogram | Script execution duration buckets. |

---

## Script cache behaviour

- The script cache is **node-local** — it is not replicated to other SwarmKeyDb
  instances. Load scripts on each node independently, or use `EVAL` (which
  caches automatically) before switching to `EVALSHA`.
- The cache is in-memory only and is cleared on restart or by `SCRIPT FLUSH`.
- SHA1 is computed from the raw UTF-8 bytes of the script source, identical to
  Redis.
