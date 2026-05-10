# HTTP REST API Gateway

SwarmKeyDb exposes Redis-compatible commands over HTTP JSON so any developer can use `curl` or `fetch()` without a Redis client.

## Configuration

- `SWARM_KEYDB_HTTP_PORT` (default: `8080`, set `0` to disable)
- `SWARM_KEYDB_HTTP_CORS_ORIGINS` (comma-separated origins, default `*`)
- `SWARM_KEYDB_REQUIREPASS` (optional; use `Authorization: Bearer <password>` or `?auth=<password>`)

## Quick curl examples

```bash
curl -sS -X POST http://localhost:8080/set/hello \
  -H 'Content-Type: application/json' \
  -d '{"value":"world"}'
# {"result":"OK"}

curl -sS http://localhost:8080/get/hello
# {"result":"world"}

curl -sS -X DELETE http://localhost:8080/del/hello
# {"result":1}
```

Additional endpoints:

- `GET /exists/{key}`
- `POST /expire/{key}` with `{"seconds":60}`
- `GET /ttl/{key}`
- `GET /keys/{pattern}`
- `POST /publish/{channel}` with `{"message":"..."}`
- `POST /xadd/{stream}` with `{"fields":{"field":"value"}}`
- `POST /cmd` with `{"cmd":"HSET","args":["hash","field","value"]}`

All responses use:

- Success: `{"result": ...}`
- Error: `{"error":"ERR ..."}`

## OpenAPI and interactive docs

- OpenAPI JSON: `GET /openapi.json`
- Swagger UI: `GET /docs`

## Browser fetch() example

```js
const baseUrl = "http://localhost:8080";

await fetch(`${baseUrl}/set/demo`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ value: "from-fetch" })
});

const getResponse = await fetch(`${baseUrl}/get/demo`);
const payload = await getResponse.json();
console.log(payload.result);
```
