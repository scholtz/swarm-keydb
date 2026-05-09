# Ethereum Bridge Tutorial

This guide walks you through deploying a `SwarmKeyDbOracle` smart contract, configuring
the SwarmKeyDb Ethereum bridge, and writing data to SwarmKeyDb from an on-chain event.

---

## Overview

SwarmKeyDb includes an **Ethereum bridge** — an opt-in background service that subscribes
to smart contract events and automatically writes/reads data from the key-value store:

```
Solidity contract          Off-chain bridge (C#)        SwarmKeyDb store
─────────────────          ────────────────────────────  ─────────────────
emit DataWriteRequested ──► EthereumBridgeService.cs ──► PUT key value
emit DataReadRequested  ──► EthereumBridgeService.cs ──► GET key
```

The bridge supports two RPC modes:
- **WebSocket** (`ws://` / `wss://`): real-time `eth_subscribe` streaming.
- **HTTP** (`http://` / `https://`): polling via `eth_getLogs` at a configurable interval.

---

## Prerequisites

- .NET 10 SDK
- Node.js 20+ (for Hardhat)
- A local Ethereum node **or** an Infura/Alchemy endpoint

---

## Step 1 — Install Hardhat and compile the contracts

```bash
cd ethereum
npm ci
npx hardhat compile
```

This compiles both `ISwarmKeyDb.sol` and `SwarmKeyDbOracle.sol`.

---

## Step 2 — Start a local Hardhat node

```bash
npx hardhat node
# Hardhat network is now running at http://127.0.0.1:8545
# The first test account private key is printed on startup — save it!
```

---

## Step 3 — Deploy the oracle contract

```bash
# Set the oracle address (the address whose private key the bridge will use to sign
# write-back transactions). For local development, use any Hardhat test account.
ORACLE_ADDRESS=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
  npx hardhat run scripts/deploy.js --network localhost
```

Note the deployed contract address printed at the end. You will need it below.

---

## Step 4 — Configure the SwarmKeyDb server

Add the following to your `appsettings.json` **or** set environment variables:

### appsettings.json

```json
{
  "Ethereum": {
    "Enabled": true,
    "RpcUrl": "ws://127.0.0.1:8545",
    "ContractAddress": "<YOUR_CONTRACT_ADDRESS>",
    "PrivateKeyHex": "<ORACLE_PRIVATE_KEY_HEX>",
    "PollIntervalSeconds": 5,
    "ReconnectDelaySeconds": 5
  }
}
```

### Environment variables

| Variable                    | Description                                             |
|-----------------------------|---------------------------------------------------------|
| `ETH_BRIDGE_ENABLED=true`   | Enable the bridge (disabled by default)                 |
| `ETH_RPC_URL`               | WebSocket or HTTP JSON-RPC endpoint                     |
| `ETH_CONTRACT_ADDRESS`      | Address of the deployed `SwarmKeyDbOracle` contract     |
| `ETH_PRIVATE_KEY`           | Hex private key for the oracle signer (optional)        |
| `ETH_POLL_INTERVAL_SECONDS` | Polling interval for HTTP mode (default: 5)             |
| `ETH_RECONNECT_DELAY_SECONDS` | Reconnect delay after failure (default: 5)            |

> **Security note:** Never commit `ETH_PRIVATE_KEY` to source control. Use a secrets
> manager or environment-specific configuration.

---

## Step 5 — Start SwarmKeyDb

```bash
# Start with the bridge enabled
ETH_BRIDGE_ENABLED=true \
ETH_RPC_URL=ws://127.0.0.1:8545 \
ETH_CONTRACT_ADDRESS=<YOUR_CONTRACT_ADDRESS> \
  dotnet run --project src/SwarmKeyDb.Server/
```

You should see log output like:

```
info: EthereumBridgeService - Ethereum bridge starting. Mode=WebSocket Contract=0x5FbDB...
info: EthereumBridgeService - Ethereum bridge WebSocket connected to ws://127.0.0.1:8545.
info: EthereumBridgeService - Ethereum bridge subscribed. SubscriptionId=0x...
```

---

## Step 6 — Write data via an on-chain event

Open a Hardhat console:

```bash
npx hardhat console --network localhost
```

Then:

```javascript
const [deployer] = await ethers.getSigners();
const oracle = await ethers.getContractAt(
  "SwarmKeyDbOracle",
  "<YOUR_CONTRACT_ADDRESS>"
);

// Request a write: the bridge will store this in SwarmKeyDb
const tx = await oracle.requestWrite(
  "nft:metadata:1",
  Buffer.from('{"name":"My NFT","description":"A decentralised NFT"}')
);
await tx.wait();
console.log("Write requested. Check SwarmKeyDb logs!");
```

---

## Step 7 — Verify in SwarmKeyDb

Use the Redis CLI (SwarmKeyDb is Redis-protocol compatible):

```bash
redis-cli -p 6379 GET "nft:metadata:1"
# → "{\"name\":\"My NFT\",\"description\":\"A decentralised NFT\"}"
```

Or check the bridge status endpoint:

```bash
curl http://localhost:8080/ethereum/bridge
```

```json
{
  "status": "connected",
  "lastProcessedBlock": 12,
  "eventCount": 1,
  "connectedSince": "2024-01-15T10:30:00Z",
  "lastError": null,
  "contractAddress": "0x5FbDB...",
  "rpcUrl": "ws://127.0.0.1:8545"
}
```

---

## Docker Compose (quick start)

A ready-to-use `docker-compose.ethereum.yml` is provided at the root of the repository:

```bash
# Pull the SwarmKeyDb image first
docker pull scholtz2/swarm-keydb:zero-day

# Set your contract address
export ETH_CONTRACT_ADDRESS=<YOUR_CONTRACT_ADDRESS>

# Start the stack
docker compose -f docker-compose.ethereum.yml up
```

---

## Running the Hardhat tests

```bash
cd ethereum
npm test
```

All tests run against an in-process Hardhat network (no external dependencies):

```
  SwarmKeyDbOracle
    ✔ sets the correct owner and oracle on deployment
    ✔ reverts when deployed with zero-address oracle
    ✔ allows the owner to update the oracle address
    ...
    ✔ full round-trip: user requests read, oracle fulfils with cached value
    14 passing
```

---

## Bridge health monitoring

The bridge exposes a `/ethereum/bridge` HTTP endpoint (on the monitoring port):

```
GET http://localhost:8080/ethereum/bridge
```

| Field               | Description                                                |
|---------------------|------------------------------------------------------------|
| `status`            | `disabled` / `connecting` / `connected` / `retrying` / `error` |
| `lastProcessedBlock`| Last Ethereum block number processed by the bridge         |
| `eventCount`        | Total number of events processed since startup             |
| `connectedSince`    | ISO 8601 timestamp when the current connection was made    |
| `lastError`         | Last error message, if any                                 |
| `contractAddress`   | Configured contract address                                |
| `rpcUrl`            | Configured RPC endpoint                                    |

---

## Architecture notes

The `EthereumBridgeService` (`src/SwarmKeyDb.Server/EthereumBridgeService.cs`) is a
background service that:

1. Reads `EthereumBridgeOptions` from configuration.
2. If `Enabled = true`, starts a polling or WebSocket loop.
3. For each received `DataWriteRequested` log, ABI-decodes the `(string key, bytes value)`
   parameters and calls `IKeyValueStore.PutAsync(key, value)`.
4. For each `DataReadRequested` log, logs the request and resolves the value from the store.
5. Exposes a `GetState()` snapshot used by the `/ethereum/bridge` monitoring endpoint.

The `KeccakHash` utility class (`src/SwarmKeyDb/KeccakHash.cs`) computes Ethereum-compatible
Keccak-256 hashes, used to derive the event topic selectors at startup.

---

## Next steps

- Implement write-back transactions: after writing to SwarmKeyDb, have the oracle call
  `dataWriteConfirmed(user, key, swarmHash)` by signing a transaction with `ETH_PRIVATE_KEY`.
- Add token-gated access: combine with the ACL middleware to restrict write access to
  specific Ethereum addresses.
- Connect to a public testnet (Sepolia): update `ETH_RPC_URL` to your Infura/Alchemy
  WebSocket endpoint and set `ETH_CONTRACT_ADDRESS` to the deployed testnet address.
