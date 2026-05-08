# Privacy-Preserving Queries

SwarmKeyDb now supports privacy-preserving key queries via HMAC-derived key tokens.

## What is protected

- Plaintext key names are never sent to the remote backend when privacy mode is enabled.
- Local scans (`keys(prefix)`, range scans, and `scan`) are resolved from a local manifest.
- Migration tooling can rewrite plaintext key namespaces into HMAC token namespaces with `swarmkeydb-migrate --enable-privacy`.

## C# usage

```csharp
var options = new SwarmKeyDbOptions
{
    PrivacyMode = PrivacyMode.ObliviousHashing,
    PrivacyKeyHex = "<64-char-hex-key>"
};

var store = new SwarmKeyValueStore(
    new BeeSwarmClient(new Uri("http://localhost:1633/"), "<batch-id>"),
    new FileKeyIndex("./data/index.json"),
    options);
```

## Migration CLI

```bash
dotnet run --project src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -- \
  --from redis://localhost:6379 \
  --to redis://localhost:6380 \
  --enable-privacy \
  --privacy-key <64-char-hex-key> \
  --dry-run
```

Then remove `--dry-run` to execute the migration.

## Privacy mode values

- `None` / `none`
- `ObliviousHashing` / `oblivious_hashing`
- `FullPSI` / `full_psi`

`FullPSI` currently provides lightweight blind-set PSI primitives (digest exchange + intersection), while storage/query APIs use the same key-token path as oblivious hashing.
