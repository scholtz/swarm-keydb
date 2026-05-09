# Roadmap for SwarmKeyDb

## K8S helm chart
- [ ] Create helm charts for smooth deployment to k8s. Create scripts to create new version of the helm chart. I want the helm charts to be published to artifacthub. The files will be in the helm chart folder in this repo and published using github pages.

## Core Functionality Enhancements
- [x] Add support for TTL and expiration of keys. (100%)
- [x] Enable batch operations for multiple get/put/delete in one call. (100%)
- [x] Introduce advanced querying capabilities like range scans and filters. (100%)
- [x] Support for composite keys and hierarchical namespaces. (100%)

## Performance and Scalability
- [x] Add in-memory caching layer for frequently accessed keys. (100%)
- [x] Implement data compression to reduce storage costs on Swarm. (100%)
- [x] Optimize indexing for faster key lookups and iterations. (100%)
- [x] Enable horizontal scaling with sharding across multiple Swarm nodes. (100%)
- [x] Add asynchronous processing for high-throughput operations. (100%)

## Multi-Instance Availability and Cache Consistency
- [ ] Create a `SwarmKeyDb.SwarmConsistency` NuGet package that validates Swarm and Bee reads with content-hash verification, feed or manifest revision checks, optional quorum policies, and operator-friendly failure diagnostics before values reach callers.
- [ ] Add consistency verification hooks to every cached `IKeyValueStore` path so cache hits, read-through fetches, and background refreshes evict or reject stale Swarm payloads instead of serving divergent in-memory data.
- [ ] Implement cross-instance cache synchronization with version stamps, invalidation events, and anti-entropy reconciliation so multiple SwarmKeyDb nodes converge after writes, expirations, restarts, and temporary network partitions.
- [ ] Define partial-resync and full-resync flows for cache state recovery so an instance can cheaply catch up when version history is available and deterministically rebuild from Swarm when it is not.
- [ ] Add multi-node integration tests and production telemetry for cache drift, verification failures, synchronization lag, and forced resync counts so operators can prove consistency during failover and rolling deployments.

## Security and Privacy
- [x] Integrate end-to-end encryption using user-provided keys. (100%)
- [x] Implement access control lists for multi-user shared databases. (100%)
- [x] Add data integrity verification with cryptographic hashes. (100%)
- [x] Support for secure key rotation and backup mechanisms. (100%)
- [x] Enable privacy-preserving queries without revealing data. (100%: added HMAC key-token privacy mode, local private key manifest-backed scans, migration `--enable-privacy`, key-token rotation helper, PSI helper primitives, SDK privacy mode flags, and docs/tutorial coverage)

## Developer Experience
- [x] Create comprehensive SDKs for JavaScript, Python, and Go. (100%: `swarm-keydb-js`, `swarm-keydb-py`, and `swarm-keydb-go` added with core API, examples, and unit tests)
- [x] Provide detailed documentation with tutorials and examples. (100%: getting-started, API reference, tutorials, SDK guides, deployment guide, FAQ, and runnable examples added with CI validation)
- [x] Add CLI tools for database management and debugging. (100%)
- [x] Implement monitoring dashboard with metrics and logs. (100%: `/metrics`, `/health`, `/ready`, `/dashboard`, structured command logging with correlation IDs)
- [x] Offer migration tools from traditional databases. (100%: `swarmkeydb-migrate` CLI supports SCAN-based import, prefix filters, dry-run, resumable checkpoints, TTL preservation, validation sampling, and Docker demo)

## Ecosystem Integration
- [x] Integrate with IPFS for hybrid storage options. (100%)
- [x] Enable interoperability with Ethereum smart contracts. (100%: `EthereumBridgeService` background service with WebSocket + HTTP polling, `ISwarmKeyDb.sol` interface, `SwarmKeyDbOracle.sol` reference implementation with Hardhat tests, `/ethereum/bridge` monitoring endpoint, `ETH_*` environment variable configuration, Docker Compose example with local Hardhat node, full unit + integration test coverage)
- [x] Support cross-chain data synchronization. (100%)
- [x] Add connectors for popular frameworks like React and Node.js. (100%: added `swarm-keydb-react` hooks/provider package with Storybook docs + tests, `swarm-keydb-node` service/middleware package with retry/pooling + tests, and runnable `examples/react-app` + `examples/node-express`)
- [x] Provide APIs for integration with decentralized identity systems. (100%: added `IDecentralizedIdentityProvider` interface with `ResolveAsync`/`AuthenticateAsync`/`CheckPermissionAsync`; `EthrDidProvider` with `did:ethr` resolution and secp256k1 personal-sign verification; `DidAuthKeyValueStore` decorator enforcing DID context on all store operations; `VerifiableCredentialAclPolicy` with operation/key-pattern VC claim evaluation; `DidAuthorizationException`; `AUTHDID` Redis command with optional proof verification; `SWARM_KEYDB_DID_MODE`, `SWARM_KEYDB_DID_RPC_URL`, `SWARM_KEYDB_DID_METHOD` env vars; DID mode indicator on `/dashboard`; `setDid`/`clearDid` in JS, Python, and Go SDKs; `docs/decentralized-identity.md` getting-started guide with DID resolution flow diagram and VC example; full unit test coverage for mock-provider grant/deny, VC policy, AUTHDID command, and dashboard indicator)
