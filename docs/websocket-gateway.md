# WebSocket Gateway

SwarmKeyDb can expose Redis-compatible commands over WebSockets so browser apps can subscribe to Pub/Sub and Streams without a TCP Redis client.

## Configuration

- `SWARM_KEYDB_WS_PORT` (default: `8765`, set to `0` to disable)
- `SWARM_KEYDB_WS_CORS_ORIGINS` (comma-separated origins, default `*`)
- `SWARM_KEYDB_REQUIREPASS` (optional password for `AUTH`)

## JSON commands (RESP2 default)

Send frames as:

```json
{"cmd":"SUBSCRIBE","args":["my-channel"]}
```

When no protocol upgrade is requested, responses are JSON:

- Success: command result encoded as JSON
- Push frame: `{"type":"push","data":[...]}`
- Error: `{"error":"ERR ...","cmd":"SUBSCRIBE"}`

## RESP3 negotiation

WebSocket connections start in RESP2 mode. You can upgrade with either JSON-array commands or raw RESP frames:

```json
["HELLO","3"]
```

Successful `HELLO 3` returns a JSON map (object), for example:

```json
{"server":"swarmkeydb","version":"7.0.0","proto":3,"id":1,"mode":"standalone","role":"master","modules":[]}
```

`HELLO 2` downgrades back to RESP2 shapes, and `RESET` restores factory state (`RESP2`, no tracking/subscriptions/transaction/auth context).

## CLIENT TRACKING push invalidation

Enable server-assisted caching over WebSocket:

```json
["CLIENT","TRACKING","ON"]
```

After another connection mutates a tracked key, the same socket receives:

```json
{"type":"push","data":["invalidate",["my-key"]]}
```

Disable with:

```json
["CLIENT","TRACKING","OFF"]
```

## Browser example

```html
<script>
const ws = new WebSocket("ws://localhost:8765/");
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  if (msg.type === "push") {
    console.log("invalidation:", msg.data);
    return;
  }
  console.log("reply:", msg);
};
ws.onopen = () => {
  ws.send(JSON.stringify(["HELLO", "3"]));
  ws.send(JSON.stringify(["CLIENT", "TRACKING", "ON"]));
  ws.send(JSON.stringify(["GET", "profile:alice"]));
};
</script>
```

## Node.js example

```js
import WebSocket from "ws";
const ws = new WebSocket("ws://localhost:8765/");
ws.on("open", () => ws.send(JSON.stringify({ cmd: "PING" })));
ws.on("message", (data) => console.log(data.toString()));
```

## React hook (`useSwarmKeyDb`) example

```tsx
import { useEffect, useMemo, useState } from "react";

export function useSwarmKeyDb(url: string, channel: string) {
  const [messages, setMessages] = useState<string[]>([]);
  const ws = useMemo(() => new WebSocket(url), [url]);

  useEffect(() => {
    ws.onopen = () => ws.send(JSON.stringify({ cmd: "SUBSCRIBE", args: [channel] }));
    ws.onmessage = (event) => setMessages((prev) => [...prev, event.data.toString()]);
    return () => ws.close();
  }, [ws, channel]);

  return messages;
}
```
