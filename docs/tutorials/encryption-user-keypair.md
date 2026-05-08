# Tutorial: Encryption with User Keypair

Goal: encrypt values at rest using a key derived from a user's Ethereum private key.

## Run

```bash
export SWARM_KEYDB_ENCRYPTION_ENABLED=true
export SWARM_KEYDB_ENCRYPTION_ETH_KEY=<64-char-hex-private-key>
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
redis-cli -p 6379 SET profile:secret "top-secret"
redis-cli -p 6379 GET profile:secret
```

## Notes

- If encryption is enabled with no key, startup fails fast.
- Reads/deletes behave normally from the client perspective.
