# Deployment

The repository includes Docker and Kubernetes assets for running SwarmKeyDb with a colocated Bee node.

## Defaults

- Deployment assets default to a Bee Sepolia testnet configuration.
- Bee is configured for Sepolia by setting `BEE_MAINNET=false`.
- Replace placeholder values for the Bee password, RPC endpoint, and postage batch id before use.

## Docker Compose

1. Copy `.env.example` to `.env` and update the placeholder values.
2. Build and start the stack:

```bash
docker compose up --build
```

The stack starts:

- `swarm-bee` on ports `1633` and `1634`
- `swarm-keydb` on port `6379`

## Redis migration demo (Docker Compose)

A self-contained migration demo is available at `deploy/migration/docker-compose.yml`.

```bash
docker compose -f deploy/migration/docker-compose.yml up --build --abort-on-container-exit
```

The migration demo provisions a source Redis container, seeds sample data, migrates keys into SwarmKeyDb, and runs validation.

## Kubernetes

Manifests are under `deploy/k8s/`.

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/secrets.example.yaml
kubectl apply -f deploy/k8s/swarm-bee.yaml
kubectl apply -f deploy/k8s/swarm-keydb.yaml
```

Bee API traffic stays internal to the cluster by default. If you need internet-reachable P2P connectivity, set a public `BEE_NAT_ADDR` and adjust the `swarm-bee-p2p` service for your cluster networking model.

## Helm

The production Helm chart is under `helm/swarm-keydb/`.

```bash
helm repo add swarm-keydb https://scholtz.github.io/swarm-keydb/
helm repo update
helm install my-swarm-keydb swarm-keydb/swarm-keydb --set env.beeUrl=http://swarm-bee-api:1633 --set secret.beePostageBatchId=<your-postage-batch-id>
```

## Monitoring

SwarmKeyDb exposes:

- `/metrics` (Prometheus)
- `/health` (liveness)
- `/ready` (readiness)
- `/dashboard` (HTML dashboard)

Default ports:

- `METRICS_PORT=9090`
- `DASHBOARD_PORT=8080`

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: swarm-keydb
    metrics_path: /metrics
    static_configs:
      - targets: ['swarm-keydb.default.svc.cluster.local:9090']
```

Example Grafana panel JSON:

```json
{
  "title": "SwarmKeyDb error rate",
  "type": "timeseries",
  "targets": [
    {
      "expr": "rate(swarmkeydb_operations_total{status=\"error\"}[5m])",
      "legendFormat": "errors/sec"
    }
  ]
}
```
