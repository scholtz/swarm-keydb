# Architecture

SwarmKeyDb exposes a Redis-compatible RESP server backed by a key index and a Swarm object store.

## Components

- `SwarmKeyDb` contains the core storage abstractions, RESP parsing and writing, and the key-value store implementation.
- `SwarmKeyDb.Server` hosts the TCP server and wires runtime configuration, logging, caching, compression, and encryption.
- `SwarmKeyDb.Tests` is a lightweight executable test runner that validates protocol and storage behavior.

## Storage flow

1. The Redis command processor translates RESP commands into `IKeyValueStore` operations.
2. The key index tracks metadata such as keys, references, and TTL state.
3. Values are stored locally for development or uploaded through Bee for Swarm-backed deployments.
4. Optional compression, encryption, and caching are layered around the core store in the server host.

## Deployment model

- Local development defaults to the file-backed Swarm client.
- Checked-in container deployment assets default to a Bee Sepolia testnet stack.
- Bee-backed deployments should provide their own postage batch, Bee password, and archival RPC endpoint.