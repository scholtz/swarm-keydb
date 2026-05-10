# WebSocket Protocol

SwarmKeyDb WebSocket endpoint: `ws://<host>:<SWARM_KEYDB_WS_PORT>/`

## Input formats

### 1) JSON

```json
{"cmd":"XREAD","args":["BLOCK","0","STREAMS","mystream","$"]}
```

- `cmd` is required string.
- `args` is optional string array.

### 2) RESP text

Raw RESP frames are accepted in text messages, for example:

```text
*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n
```

## Output formats

- If command input is JSON, responses are JSON frames.
- If command input is RESP, responses are RESP frames.

JSON response shapes:

- Success: `{"cmd":"PING","data":"PONG"}`
- Push delivery: `{"push":[...]}`
- Error: `{"error":"ERR ...","cmd":"PING"}`

## Auth

If `SWARM_KEYDB_REQUIREPASS` is set:

```json
{"cmd":"AUTH","args":["<password>"]}
```

Non-`AUTH` commands return:

```json
{"error":"NOAUTH Authentication required.","cmd":"GET"}
```

## Origin filtering

`SWARM_KEYDB_WS_CORS_ORIGINS` controls allowed `Origin` header values for upgrades.
Disallowed origins receive HTTP `403`.
