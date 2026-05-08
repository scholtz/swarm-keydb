# Development

## Requirements

- .NET SDK 10.0.102 or later in the .NET 10 feature band

## Build and test

```bash
dotnet restore SwarmKeyDb.slnx
dotnet build SwarmKeyDb.slnx
dotnet run --project tests/SwarmKeyDb.Tests/SwarmKeyDb.Tests.csproj
```

## Local server workflow

Run the server with the local file-backed backend:

```bash
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
redis-cli -p 6379 SET profile:name Ada
redis-cli -p 6379 GET profile:name
```

To use Bee instead, set `SWARM_KEYDB_BACKEND=bee` together with `BEE_URL` and `BEE_POSTAGE_BATCH_ID`.

## CLI workflow

Run the CLI from source:

```bash
dotnet run --project src/SwarmKeyDb.Cli/SwarmKeyDb.Cli.csproj -- --help
```

## Documentation expectation

When runtime behavior, deployment, configuration, or workflows change, update the relevant files in `docs/` and any impacted top-level documentation in the same change.
