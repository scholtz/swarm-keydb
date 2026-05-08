# Tutorial: Shared Config with Multi-user ACL

Goal: allow one address full access and one address read-only access.

## Run

```bash
export SWARM_KEYDB_ACL_ENABLED=true
export SWARM_KEYDB_ACL_MODE=allowlist
export SWARM_KEYDB_ACL_ENTRIES='[{"address":"0x1111111111111111111111111111111111111111","permission":"admin"},{"address":"0x2222222222222222222222222222222222222222","permission":"read"}]'
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

Then in Redis clients:

```text
AUTHADDR 0x1111111111111111111111111111111111111111
SET shared:feature-x enabled
AUTHADDR 0x2222222222222222222222222222222222222222
GET shared:feature-x
```
