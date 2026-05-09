# SwarmConsistency NuGet Publish Pipeline

SwarmKeyDb publishes `SwarmKeyDb.SwarmConsistency` from GitHub Actions using workflow `.github/workflows/nuget-swarmconsistency-publish.yml`.

## Trigger model

The workflow runs on:

- Push to `main`
- Manual trigger (`workflow_dispatch`)

## Package versioning

The workflow uses UTC datetime-based versioning:

- `1.0.YYYYMMDD.HHMMSS`
- Example: `1.0.20260509.143601`

> `1.0.20260509143601` is not accepted by `dotnet`/NuGet as a valid version string, so the datetime is split into patch + revision while keeping deterministic UTC timestamp semantics.

## Required GitHub repository settings

Set these in **GitHub repository settings** before running the workflow.

### Secrets

- `NUGET_API_KEY` (required)
  - API key from NuGet.org with push permission for `SwarmKeyDb.SwarmConsistency`.

### Variables

- `NUGET_PACKAGE_ID` (optional)
  - Default: `SwarmKeyDb.SwarmConsistency`
  - Override only if publishing under a different package id.

- `NUGET_SOURCE_URL` (optional)
  - Default: `https://api.nuget.org/v3/index.json`
  - Override only for private/internal feeds.

## What the workflow does

1. Checks out repository code.
2. Sets up .NET 10 SDK.
3. Computes version as `1.0.YYYYMMDD.HHMMSS` (UTC).
4. Restores `src/SwarmKeyDb.SwarmConsistency/SwarmKeyDb.SwarmConsistency.csproj`.
5. Packs the project into `./artifacts/nuget`.
6. Pushes `.nupkg` to configured NuGet source with `--skip-duplicate`.

## Manual publish steps

1. Open GitHub Actions.
2. Run workflow `Publish SwarmConsistency NuGet`.
3. Confirm package appears at NuGet.org package page.

## Local verification command

```bash
dotnet pack src/SwarmKeyDb.SwarmConsistency/SwarmKeyDb.SwarmConsistency.csproj -c Release -o ./artifacts/nuget /p:Version=1.0.20260509.143601
```
