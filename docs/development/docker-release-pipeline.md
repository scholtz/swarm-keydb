# Docker Release Pipelines (GitHub Actions)

SwarmKeyDb publishes Docker images to Docker Hub as part of CI/CD.

## Published tags

On every successful push pipeline on `main`, CI publishes:

- `scholtz2/swarm-keydb:zero-day` (rolling tag for the newest validated pipeline build)
- `scholtz2/swarm-keydb:release-YYYYMMDD` (dated release tag in UTC)

`latest` is intentionally not updated automatically.

## Stable promotion flow

Use the manual workflow **Promote Release Tag To Latest** to retag a validated release image:

1. Open GitHub Actions.
2. Run workflow `Promote Release Tag To Latest`.
3. Provide `release_date` in `YYYYMMDD` format.
4. The workflow promotes `scholtz2/swarm-keydb:release-YYYYMMDD` to `scholtz2/swarm-keydb:latest`.

This keeps `latest` as an explicit stability channel.

## Required GitHub settings

Configure these in repository settings before running publish/promotion workflows.

### Secrets

- `DOCKERHUB_TOKEN` (required)
  - Docker Hub access token for push permissions on `scholtz2/swarm-keydb`.
  - Create this in Docker Hub under Account Settings > Personal Access Tokens.

### Variables

- `DOCKERHUB_USERNAME` (required)
  - Set to `scholtz2`.

- `DOCKER_IMAGE_NAME` (optional)
  - Default is `swarm-keydb`.
  - Keep this value unless you intentionally publish under a different repository name.

## Workflow files

- `.github/workflows/ci.yml`
  - Runs build/test matrix and publishes Docker tags on pushes to `main`.

- `.github/workflows/promote-release-latest.yml`
  - Manual promotion workflow for `release-YYYYMMDD` to `latest`.
