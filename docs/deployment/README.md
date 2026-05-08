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