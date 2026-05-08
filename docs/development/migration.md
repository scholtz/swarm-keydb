# Redis migration CLI

`swarmkeydb-migrate` migrates keys from a Redis-compatible source into SwarmKeyDb over Redis protocol connections.

## Run from source

```bash
dotnet run --project src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -- \
  --from redis://localhost:6379 \
  --to redis://localhost:6380
```

## Common scenarios

- Full migration:
  ```bash
  dotnet run --project src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -- --from redis://source:6379 --to redis://swarmkeydb:6380
  ```
- Prefix-only migration:
  ```bash
  dotnet run --project src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -- --from redis://source:6379 --to redis://swarmkeydb:6380 --prefix user:
  ```
- Dry run sizing pass:
  ```bash
  dotnet run --project src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -- --from redis://source:6379 --to redis://swarmkeydb:6380 --dry-run
  ```
- Validation after import:
  ```bash
  dotnet run --project src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -- --from redis://source:6379 --to redis://swarmkeydb:6380 --validate --validate-sample-percent 5
  ```
- Enable privacy-preserving key migration (HMAC token keys):
  ```bash
  dotnet run --project src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -- --from redis://source:6379 --to redis://swarmkeydb:6380 --enable-privacy --privacy-key <64-char-hex-key> --dry-run
  ```

## TTL handling

The migrator reads TTL from source keys and writes destination keys with the same TTL. Validation compares destination TTL within a 1-second tolerance.

## Resume after interruption

Migration state is written to `.swarmkeydb-migrate.checkpoint.json` by default. Re-run the same command to resume from the saved cursor and in-flight batch index.
