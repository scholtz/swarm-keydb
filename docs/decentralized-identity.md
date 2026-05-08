# Decentralized Identity (DID) Integration

SwarmKeyDB supports [W3C DID](https://www.w3.org/TR/did-core/) based authentication, letting dApp developers tie database access control to a user's decentralized identity rather than raw Ethereum keypairs alone.

---

## Concepts

| Term | Description |
|------|-------------|
| **DID** | Decentralized Identifier — a URI that resolves to a DID Document (e.g. `did:ethr:0x…`) |
| **DID Document** | Machine-readable JSON document describing a DID, its controller, and verification methods |
| **Proof** | Cryptographic evidence that a caller controls a DID (Ethereum personal-sign in the `ethr` method) |
| **Verifiable Credential (VC)** | Signed statement about a subject DID that can be used for fine-grained access control |

---

## Quick Start

### C# — enable DID auth on a store

```csharp
var store = new SwarmKeyValueStore(client, index, new SwarmKeyDbOptions
{
    DidMode   = DidAuthMode.EthrDid,
    DidRpcUrl = "https://mainnet.infura.io/v3/<API_KEY>",  // optional, for on-chain resolution
});
```

When `DidMode != None` every `put`/`get`/`delete` operation requires a valid DID context to be set on the current async call chain (via `IDidContextAccessor`). Unauthenticated calls throw `DidAuthorizationException` (HTTP 403).

### Redis Protocol — `AUTHDID` command

```
AUTHDID <did> [<proof_message> <proof_signature>]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `did` | Yes | DID string, e.g. `did:ethr:0xf39F…` |
| `proof_message` | No | Plain-text challenge that was signed |
| `proof_signature` | No | Hex-encoded 65-byte Ethereum personal-sign signature |

When `proof_message` and `proof_signature` are supplied the server verifies the signature immediately and rejects the command (with an error response) if it is invalid.  Once `AUTHDID` succeeds, all subsequent commands on that connection are executed under the registered DID.

```bash
redis-cli -p 6379 AUTHDID "did:ethr:0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
redis-cli -p 6379 SET mykey value
```

---

## DID Resolution Flow

```
Caller                       SwarmKeyDB                    Ethereum Node (optional)
  │                               │                                  │
  │──AUTHDID did:ethr:0x… sig──► │                                  │
  │                               │── parse did:ethr:<address> ──►  │
  │                               │                                  │
  │                               │── recover address from sig ──    │
  │                               │   (secp256k1 ECDSA locally)      │
  │                               │                                  │
  │                               │── eth_call ownerOf(address) ──► │  (if RPC configured)
  │                               │◄── controller address ─────────  │
  │                               │                                  │
  │◄── +OK ──────────────────────  │
  │                               │
  │── SET key value ─────────── ► │
  │                               │── CheckPermission(did, key, Write)
  │◄── +OK ──────────────────────  │
```

---

## Configuration

### Server (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `SWARM_KEYDB_DID_MODE` | `None` | DID auth mode: `None` or `EthrDid` |
| `SWARM_KEYDB_DID_RPC_URL` | _(empty)_ | Ethereum JSON-RPC URL for on-chain DID controller lookup |
| `SWARM_KEYDB_DID_METHOD` | `ethr` | DID method (reserved for future extension) |

### appsettings.json alternative

```json
{
  "Did": {
    "Mode": "EthrDid",
    "RpcUrl": "https://mainnet.infura.io/v3/<API_KEY>",
    "Method": "ethr"
  }
}
```

---

## SDK Usage

### JavaScript / Node.js

```js
import { SwarmKeyDb, DidAuthMode } from 'swarm-keydb';

const db = new SwarmKeyDb({
  host: 'localhost',
  port: 6379,
  didMode: DidAuthMode.EthrDid,
  didRpcUrl: 'http://localhost:8545',
});

await db.connect();

// Register DID — with optional Ethereum personal-sign proof
await db.setDid(
  'did:ethr:0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266',
  'challenge-message',   // proof_message
  '0x<hex-signature>'   // proof_signature
);

await db.put('profile:name', 'Alice');
const value = await db.get('profile:name');

db.clearDid(); // remove DID context from this instance
```

### Python

```python
from swarm_keydb import SwarmKeyDb
from swarm_keydb.client import DidAuthMode

db = SwarmKeyDb(
    host='localhost',
    port=6379,
    did_mode=DidAuthMode.ETHR_DID,
    did_rpc_url='http://localhost:8545',
)

# Register DID — with optional Ethereum personal-sign proof
db.set_did(
    'did:ethr:0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266',
    proof_message='challenge-message',
    proof_signature='0x<hex-signature>',
)

db.put('profile:name', 'Alice')
value = db.get('profile:name')

db.clear_did()  # remove DID context
```

### Go

```go
client := swarmkeydb.NewWithRedisClientAndOptions(rdb, swarmkeydb.Options{
    DidMode:   swarmkeydb.DidAuthModeEthrDid,
    DidRpcUrl: "http://localhost:8545",
})

// Register DID — with optional Ethereum personal-sign proof
if err := client.SetDid(ctx,
    "did:ethr:0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    "challenge-message",  // proof_message (empty = no proof)
    "0x<hex-signature>",  // proof_signature
); err != nil {
    log.Fatal(err)
}

if err := client.Put(ctx, "profile:name", "Alice"); err != nil {
    log.Fatal(err)
}

client.ClearDid() // remove DID context from this instance
```

---

## Verifiable Credential ACL Policy

For fine-grained access delegation you can use `VerifiableCredentialAclPolicy`.  A Verifiable Credential grants a subject DID permission to perform a specific operation on keys matching a pattern:

```csharp
var policy = new VerifiableCredentialAclPolicy();

var vc = new VerifiableCredential
{
    Issuer      = "did:ethr:0x<issuer>",
    SubjectDids = ["did:ethr:0x<user>"],
    Claims      = new Dictionary<string, string>
    {
        ["operation"]  = "read",       // read | write | delete | *
        ["keyPattern"] = "profile:",   // optional key prefix filter
    },
    ExpiresAt = DateTimeOffset.UtcNow.AddDays(30),
};

bool allowed = await policy.IsAllowedAsync(
    "did:ethr:0x<user>",
    "profile:name",
    DidOperation.Read,
    new[] { vc });
```

VC claims recognised by the policy:

| Claim | Values | Default |
|-------|--------|---------|
| `operation` | `read`, `write`, `delete`, `*` | _(all, if absent)_ |
| `keyPattern` | Key prefix string | _(all keys, if absent)_ |

---

## Error Handling

| Exception | Status code | When thrown |
|-----------|-------------|-------------|
| `DidAuthorizationException` | 403 | No DID context, invalid proof, or permission denied |

Via the Redis protocol the server returns an `ERR` response:

```
-ERR DID authorization required: no DID context is set.
-ERR DID authorization failed: the proof presented for 'did:ethr:0x…' is invalid.
-ERR DID authorization denied: 'did:ethr:0x…' does not have read permission on 'key'.
```

---

## Running the Example (local Hardhat/Anvil node)

> **Note:** A full Docker-Compose runnable example (`examples/did-auth/`) is planned as a follow-up.

For now, the integration can be exercised manually:

```bash
# 1. Start Anvil (local Ethereum node)
anvil

# 2. Start SwarmKeyDB with DID mode enabled
SWARM_KEYDB_DID_MODE=EthrDid \
SWARM_KEYDB_DID_RPC_URL=http://127.0.0.1:8545 \
dotnet run --project src/SwarmKeyDb.Server/

# 3. Connect via redis-cli and authenticate
redis-cli -p 6379 AUTHDID "did:ethr:0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
redis-cli -p 6379 SET mykey value
redis-cli -p 6379 GET mykey
```

---

## Extending: Custom DID Provider

Implement `IDecentralizedIdentityProvider` to support additional DID methods:

```csharp
public sealed class MyCustomDidProvider : IDecentralizedIdentityProvider
{
    public Task<DidDocument?> ResolveAsync(string did, CancellationToken ct = default)
    {
        // parse DID, build DID document ...
        return Task.FromResult<DidDocument?>(doc);
    }

    public Task<bool> AuthenticateAsync(string did, DidProof proof, CancellationToken ct = default)
    {
        // verify proof against the DID document ...
        return Task.FromResult(true);
    }

    public Task<bool> CheckPermissionAsync(string did, string key, DidOperation op, CancellationToken ct = default)
    {
        // custom ACL logic ...
        return Task.FromResult(true);
    }
}
```

Register it before calling `AddSwarmKeyDb`:

```csharp
services.AddSingleton<IDecentralizedIdentityProvider, MyCustomDidProvider>();
```
