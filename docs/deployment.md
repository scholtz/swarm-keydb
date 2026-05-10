# Deployment Guide

Use this guide for production-style Docker deployment and operational checks.

> [!IMPORTANT]
> Bee-backed writes require a valid postage batch id. Without it, writes fail.

## Docker run

```bash
docker pull scholtz2/swarm-keydb:zero-day
docker run --rm -p 6379:6379 -p 8080:8080 -p 9090:9090 \
  -e SWARM_KEYDB_BACKEND=bee \
  -e BEE_URL=http://host.docker.internal:1633/ \
  -e BEE_POSTAGE_BATCH_ID=<your-postage-batch-id> \
  -e METRICS_ENABLED=true \
  -e DASHBOARD_ENABLED=true \
  scholtz2/swarm-keydb:zero-day
```

## Required environment variables

- `SWARM_KEYDB_BACKEND` / `BACKEND` (`local`, `bee`, `swarm`, `ipfs`, or `hybrid`)
- `BEE_URL` (required when backend is `bee`)
- `BEE_POSTAGE_BATCH_ID` (required when backend is `bee`)
- `IPFS_API_URL` (required when backend is `ipfs` or `hybrid`)

## Recommended production environment variables

- `SWARM_KEYDB_BIND=0.0.0.0`
- `SWARM_KEYDB_PORT=6379`
- `SWARM_KEYDB_CACHE_ENABLED=true`
- `SWARM_KEYDB_ASYNC_ENABLED=true`
- `LOG_LEVEL=Information`
- `METRICS_ENABLED=true`, `METRICS_PORT=9090`
- `DASHBOARD_ENABLED=true`, `DASHBOARD_PORT=8081`

See full variable reference in `docs/reference/configuration.md`.

## Health checks

```bash
curl http://localhost:8081/health
curl http://localhost:8081/ready
curl http://localhost:8081/backend
```

## Prometheus integration

```yaml
scrape_configs:
  - job_name: swarm-keydb
    metrics_path: /metrics
    static_configs:
      - targets: ['swarm-keydb:9090']
```

## Existing deployment assets

- Docker Compose + Bee: `docs/deployment/README.md`
- Sharded 3-node example (local backend): `examples/sharding/`
- Kubernetes manifests: `deploy/k8s/`
- Helm chart: `helm/swarm-keydb/` (published at `https://scholtz.github.io/swarm-keydb/`)

## Helm quick reference

```bash
helm repo add ethersphere https://ethersphere.github.io/helm
helm repo update
helm install --generate-name ethersphere/bee --namespace swarm-keydb --create-namespace

helm repo add swarm-keydb https://scholtz.github.io/swarm-keydb/
helm repo update
helm upgrade --install swarm-keydb swarm-keydb/swarm-keydb \
  --namespace swarm-keydb \
  --create-namespace \
  --set secret.beePostageBatchId=<your-postage-batch-id>
```

Default chart behavior uses `https://bzz.limo` for `env.beeUrl`. Override it with your Bee service DNS name if you want SwarmKeyDb to use the Bee pod you installed in-cluster.

For a full Bee + SwarmKeyDb Helm walkthrough, see `docs/deployment/README.md`.
