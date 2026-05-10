# WebSocket Gateway

SwarmKeyDb can expose Redis-compatible commands over WebSockets so browser apps can subscribe to Pub/Sub and Streams without a TCP Redis client.

## Configuration

- `SWARM_KEYDB_WS_PORT` (default: `8765`, set to `0` to disable)
- `SWARM_KEYDB_WS_CORS_ORIGINS` (comma-separated origins, default `*`)
- `SWARM_KEYDB_REQUIREPASS` (optional password for `AUTH`)

## JSON commands

Send frames as:

```json
{"cmd":"SUBSCRIBE","args":["my-channel"]}
```

Responses are JSON:

- Success: `{"cmd":"SUBSCRIBE","data":[...]}`
- Push frame: `{"push":[...]}`
- Error: `{"error":"ERR ...","cmd":"SUBSCRIBE"}`

## Browser example

```html
<script>
const ws = new WebSocket("ws://localhost:8765/");
ws.onmessage = (event) => console.log("ws:", event.data);
ws.onopen = () => {
  ws.send(JSON.stringify({ cmd: "SUBSCRIBE", args: ["demo-channel"] }));
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
