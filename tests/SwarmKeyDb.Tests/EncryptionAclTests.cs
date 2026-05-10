using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using SwarmKeyDb;
using SwarmKeyDb.Cli;
using SwarmKeyDb.Migrate;
using SwarmKeyDb.SwarmConsistency;
using SwarmKeyDb.Server;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

[TestFixture]
public class EncryptionAclTests
{
    [Test]
    public async Task CompressingKeyValueStorePutStoresCompressedValueAsync()
    {
        var inner = new CountingKeyValueStore();
        var store = CreateCompressingStore(inner, minSizeBytes: 0);
        var original = Encoding.UTF8.GetBytes(new string('x', 200));

        await store.PutAsync("compressed:key", original);

        var stored = await inner.GetAsync("compressed:key")
            ?? throw new InvalidOperationException("Inner store should have data.");
        Assert(!stored.SequenceEqual(original), "Stored bytes should be compressed (different from original).");
        // GZip magic
        Assert(stored[0] == 0x1F && stored[1] == 0x8B, "Stored data should start with GZip magic bytes.");
    }

    [Test]
    public async Task CompressingKeyValueStoreGetReturnsDecompressedValueAsync()
    {
        var inner = new CountingKeyValueStore();
        var store = CreateCompressingStore(inner, minSizeBytes: 0);
        var original = Encoding.UTF8.GetBytes(new string('y', 200));

        await store.PutAsync("roundtrip:key", original);
        var retrieved = await store.GetAsync("roundtrip:key");

        Assert(retrieved is not null, "Retrieved value should not be null.");
        Assert(retrieved!.SequenceEqual(original), "Retrieved value should equal the original.");
    }

    [Test]
    public async Task CompressingKeyValueStoreSkipsCompressionBelowMinSizeAsync()
    {
        var inner = new CountingKeyValueStore();
        var store = CreateCompressingStore(inner, minSizeBytes: 100);
        var small = Encoding.UTF8.GetBytes("tiny");

        await store.PutAsync("small:key", small);

        var stored = await inner.GetAsync("small:key");
        Assert(stored is not null, "Inner store should have data.");
        Assert(stored!.SequenceEqual(small), "Small value should be stored uncompressed.");
    }

    [Test]
    public async Task CompressingKeyValueStoreHandlesLegacyUncompressedDataAsync()
    {
        var inner = new CountingKeyValueStore();
        var legacy = Encoding.UTF8.GetBytes("legacy-plain-text-value");
        // Directly put uncompressed data into the inner store (simulates old data)
        await inner.PutAsync("legacy:key", legacy);

        var store = CreateCompressingStore(inner, minSizeBytes: 0);
        var retrieved = await store.GetAsync("legacy:key");

        Assert(retrieved is not null, "Legacy value should be readable.");
        Assert(retrieved!.SequenceEqual(legacy), "Legacy uncompressed value should be returned as-is.");
    }

    [Test]
    public async Task CompressingKeyValueStoreBrotliCompressAndDecompressAsync()
    {
        var inner = new CountingKeyValueStore();
        var store = CreateCompressingStore(inner, algorithm: CompressionAlgorithm.Brotli, minSizeBytes: 0);
        var original = Encoding.UTF8.GetBytes(new string('z', 200));

        await store.PutAsync("brotli:key", original);
        var stored = await inner.GetAsync("brotli:key")
            ?? throw new InvalidOperationException("Inner store should have data.");

        Assert(!stored.SequenceEqual(original), "Stored bytes should be compressed.");
        // Custom magic prefix 0xCE 0xB8 (SwarmKeyDb-specific Brotli wrapper)
        Assert(stored[0] == 0xCE && stored[1] == 0xB8, "Stored data should start with custom Brotli magic bytes.");

        var retrieved = await store.GetAsync("brotli:key");
        Assert(retrieved is not null, "Retrieved value should not be null.");
        Assert(retrieved!.SequenceEqual(original), "Brotli roundtrip should return original value.");
    }

    [Test]
    public async Task CompressingKeyValueStoreDeleteAndTtlPassThroughAsync()
    {
        var inner = new CountingKeyValueStore();
        var store = CreateCompressingStore(inner, minSizeBytes: 0);
        var value = Encoding.UTF8.GetBytes("hello");

        await store.PutAsync("pass:key", value);
        Assert(await store.SetTtlAsync("pass:key", TimeSpan.FromMinutes(1)), "SetTtlAsync should pass through.");

        var (exists, ttl) = await store.GetTtlAsync("pass:key");
        Assert(exists, "GetTtlAsync should show key exists.");
        Assert(ttl is not null, "GetTtlAsync should return a TTL.");

        Assert(await store.RemoveTtlAsync("pass:key"), "RemoveTtlAsync should pass through.");
        Assert(await store.DeleteAsync("pass:key"), "DeleteAsync should pass through.");

        var keys = await store.ListKeysAsync();
        Assert(!keys.Contains("pass:key"), "ListKeysAsync should show key deleted.");
    }

    [Test]
    public async Task EncryptingKeyValueStorePutStoresEncryptedValueAsync()
    {
        var inner = new CountingKeyValueStore();
        var keyHex = MakeKeyHex();
        var store = CreateEncryptingStore(inner, keyHex: keyHex);
        var original = System.Text.Encoding.UTF8.GetBytes("secret value");

        await store.PutAsync("enc:key", original);

        var stored = await inner.GetAsync("enc:key")
            ?? throw new InvalidOperationException("Inner store should have data.");
        Assert(!stored.SequenceEqual(original), "Stored bytes should be encrypted (different from original).");
        // Magic bytes 0xAE 0x73
        Assert(stored[0] == 0xAE && stored[1] == 0x73, "Stored data should start with encryption magic bytes 0xAE 0x73.");
    }

    [Test]
    public async Task EncryptingKeyValueStoreGetReturnsDecryptedValueAsync()
    {
        var inner = new CountingKeyValueStore();
        var keyHex = MakeKeyHex();
        var store = CreateEncryptingStore(inner, keyHex: keyHex);
        var original = System.Text.Encoding.UTF8.GetBytes("another secret");

        await store.PutAsync("dec:key", original);
        var retrieved = await store.GetAsync("dec:key");

        Assert(retrieved is not null, "Retrieved value should not be null.");
        Assert(retrieved!.SequenceEqual(original), "Decrypted value should equal the original plaintext.");
    }

    [Test]
    public async Task EncryptingKeyValueStoreNonceIsRandomSameValueDifferentCiphertextAsync()
    {
        var inner = new CountingKeyValueStore();
        var keyHex = MakeKeyHex();
        var store = CreateEncryptingStore(inner, keyHex: keyHex);
        var original = System.Text.Encoding.UTF8.GetBytes("determinism test");

        await store.PutAsync("nonce:key", original);
        var first = (await inner.GetAsync("nonce:key"))!.ToArray();

        await store.PutAsync("nonce:key", original);
        var second = (await inner.GetAsync("nonce:key"))!.ToArray();

        Assert(!first.SequenceEqual(second), "Two encryptions of the same value must produce different ciphertext (random nonce).");
    }

    [Test]
    public async Task EncryptingKeyValueStoreLegacyUnencryptedDataReturnedUnchangedAsync()
    {
        var inner = new CountingKeyValueStore();
        var keyHex = MakeKeyHex();
        var legacy = System.Text.Encoding.UTF8.GetBytes("old plaintext value");
        // Store raw bytes directly in the inner store (no magic bytes).
        await inner.PutAsync("legacy:key", legacy);

        var store = CreateEncryptingStore(inner, keyHex: keyHex);
        var retrieved = await store.GetAsync("legacy:key");

        Assert(retrieved is not null, "Legacy value should be readable.");
        Assert(retrieved!.SequenceEqual(legacy), "Legacy unencrypted value should be returned as-is.");
    }

    [Test]
    public async Task EncryptingKeyValueStoreTamperedCiphertextThrowsCryptographicExceptionAsync()
    {
        var inner = new CountingKeyValueStore();
        var keyHex = MakeKeyHex();
        var store = CreateEncryptingStore(inner, keyHex: keyHex);
        var original = System.Text.Encoding.UTF8.GetBytes("tamper test");

        await store.PutAsync("tamper:key", original);

        // Flip a bit in the stored ciphertext (after the 30-byte header).
        var stored = (await inner.GetAsync("tamper:key"))!;
        stored[stored.Length - 1] ^= 0xFF;
        await inner.PutAsync("tamper:key", stored);

        var threw = false;
        try
        {
            await store.GetAsync("tamper:key");
        }
        catch (System.Security.Cryptography.CryptographicException)
        {
            threw = true;
        }

        Assert(threw, "Tampered ciphertext must throw CryptographicException (GCM tag verification failure).");
    }

    [Test]
    public async Task EncryptingKeyValueStoreDeleteAndTtlPassThroughAsync()
    {
        var inner = new CountingKeyValueStore();
        var keyHex = MakeKeyHex();
        var store = CreateEncryptingStore(inner, keyHex: keyHex);
        var value = System.Text.Encoding.UTF8.GetBytes("ttl test");

        await store.PutAsync("pass:key", value);
        Assert(await store.SetTtlAsync("pass:key", TimeSpan.FromMinutes(1)), "SetTtlAsync should pass through.");

        var (exists, ttl) = await store.GetTtlAsync("pass:key");
        Assert(exists, "GetTtlAsync should show key exists.");
        Assert(ttl is not null, "GetTtlAsync should return a TTL.");

        Assert(await store.RemoveTtlAsync("pass:key"), "RemoveTtlAsync should pass through.");
        Assert(await store.DeleteAsync("pass:key"), "DeleteAsync should pass through.");

        var keys = await store.ListKeysAsync();
        Assert(!keys.Contains("pass:key"), "ListKeysAsync should show key deleted.");
    }

    [Test]
    public Task EncryptingKeyValueStoreEthereumKeyDerivationProducesConsistentKeyAsync()
    {
        // A well-known Ethereum private key (test vector, NOT for production use).
        var ethPrivKeyHex = "4c0883a69102937d6231471b5dbb6e538eba2ef45d64b07157e4bcef88d9bdba";

        var key1 = EncryptingKeyValueStore.DeriveKeyFromEthPrivateKey(ethPrivKeyHex);
        var key2 = EncryptingKeyValueStore.DeriveKeyFromEthPrivateKey(ethPrivKeyHex);

        Assert(key1.Length == 32, "Derived key should be 32 bytes.");
        Assert(key1.SequenceEqual(key2), "Same Ethereum private key must always derive the same AES key.");

        // Different Ethereum key must produce a different AES key.
        var differentEthKey = "1c0883a69102937d6231471b5dbb6e538eba2ef45d64b07157e4bcef88d9bdba";
        var key3 = EncryptingKeyValueStore.DeriveKeyFromEthPrivateKey(differentEthKey);
        Assert(!key1.SequenceEqual(key3), "Different Ethereum keys must derive different AES keys.");

        return Task.CompletedTask;
    }

    [Test]
    public Task EncryptingKeyValueStoreStartupFailsWhenEnabledWithNoKeyAsync()
    {
        var inner = new CountingKeyValueStore();
        var options = Options.Create(new EncryptionOptions { Enabled = true, KeyHex = null, EthPrivateKeyHex = null });

        var threw = false;
        try
        {
            _ = new EncryptingKeyValueStore(inner, options, NullLogger<EncryptingKeyValueStore>.Instance);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("no key is configured"))
        {
            threw = true;
        }

        Assert(threw, "Constructor must throw InvalidOperationException when encryption is enabled but no key is configured.");
        return Task.CompletedTask;
    }

    [Test]
    public async Task BackupAndRestoreServicesRoundTripEncryptedSnapshotAsync()
    {
        var swarm = new MutableSwarmClient();
        var sourceKey = MakeKeyHex();
        var sourceProvider = new MutableEncryptionKeyProvider(new EncryptionOptions
        {
            Enabled = true,
            EthPrivateKeyHex = sourceKey
        });
        var sourceStore = new EncryptingKeyValueStore(
            new SwarmKeyValueStore(swarm, new InMemoryKeyIndex()),
            sourceProvider,
            NullLogger<EncryptingKeyValueStore>.Instance);

        await sourceStore.PutAsync("secure:key", Encoding.UTF8.GetBytes("backup-secret"));
        await sourceStore.SetTtlAsync("secure:key", TimeSpan.FromSeconds(30));

        var backupService = new BackupService(sourceStore, swarm, sourceProvider);
        var backupResult = await backupService.BackupAsync();
        Assert(backupResult.Reference.StartsWith("swarm://", StringComparison.Ordinal), "Backup should return a swarm:// URI.");

        var snapshotPayload = await swarm.DownloadAsync(backupResult.Reference["swarm://".Length..]);
        Assert(snapshotPayload.Take(4).SequenceEqual(new byte[] { 0x53, 0x4B, 0x42, 0x31 }), "Encrypted backups should use the snapshot envelope header.");

        var restoredProvider = new MutableEncryptionKeyProvider(new EncryptionOptions());
        var restoredStore = new EncryptingKeyValueStore(
            new SwarmKeyValueStore(swarm, new InMemoryKeyIndex()),
            restoredProvider,
            NullLogger<EncryptingKeyValueStore>.Instance);
        var restoreService = new RestoreService(new BackupService(restoredStore, swarm, restoredProvider), restoredStore, restoredProvider);
        var restoreResult = await restoreService.RestoreAsync(backupResult.Reference, sourceKey);

        AssertEqual(1, restoreResult.RestoredKeyCount);
        var restoredValue = await restoredStore.GetAsync("secure:key");
        AssertEqual("backup-secret", Encoding.UTF8.GetString(restoredValue!));
        var ttl = await restoredStore.GetTtlAsync("secure:key");
        Assert(ttl.Exists && ttl.Ttl is not null && ttl.Ttl.Value > TimeSpan.Zero, "Restored TTL should remain positive.");
    }

    [Test]
    public async Task KeyRotationServiceRewritesEncryptedValuesUnderNewKeyAsync()
    {
        var swarm = new MutableSwarmClient();
        var index = new InMemoryKeyIndex();
        var oldKey = MakeKeyHex();
        var newKey = MakeKeyHex();
        var provider = new MutableEncryptionKeyProvider(new EncryptionOptions
        {
            Enabled = true,
            EthPrivateKeyHex = oldKey
        });
        var store = new EncryptingKeyValueStore(
            new SwarmKeyValueStore(swarm, index),
            provider,
            NullLogger<EncryptingKeyValueStore>.Instance);
        await store.PutAsync("rotate:key", Encoding.UTF8.GetBytes("rotated-secret"));

        var originalReference = await index.GetReferenceAsync("rotate:key");
        var rotationService = new KeyRotationService(store, swarm, provider, new BackupService(store, swarm, provider));
        var result = await rotationService.RotateAsync(oldKey, newKey);

        Assert(result.ManifestReference.StartsWith("swarm://", StringComparison.Ordinal), "Rotation should publish a manifest URI.");
        var rotatedReference = await index.GetReferenceAsync("rotate:key");
        Assert(originalReference is not null && rotatedReference is not null && !string.Equals(originalReference, rotatedReference, StringComparison.Ordinal),
            "Rotation should rewrite the stored reference under the new key.");
        AssertEqual("rotated-secret", Encoding.UTF8.GetString((await store.GetAsync("rotate:key"))!));

        provider.Update(new EncryptionOptions
        {
            Enabled = true,
            EthPrivateKeyHex = oldKey
        });

        var threw = false;
        try
        {
            _ = await store.GetAsync("rotate:key");
        }
        catch (System.Security.Cryptography.CryptographicException)
        {
            threw = true;
        }

        Assert(threw, "Old key should no longer decrypt rotated values.");
    }

    [Test]
    public async Task AclAllowlistReadAddressCanGetAsync()
    {
        var inner = new CountingKeyValueStore();
        await inner.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
        var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress };
        var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
            new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Read });

        var value = await store.GetAsync("shared:key");

        AssertEqual("value", Encoding.UTF8.GetString(value!));
    }

    [Test]
    public async Task AclAllowlistWriteAddressCanPutAndDeleteAsync()
    {
        var inner = new CountingKeyValueStore();
        var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress };
        var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
            new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Write });

        await store.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
        Assert(await store.DeleteAsync("shared:key"), "Write permission should allow delete.");
    }

    [Test]
    public async Task AclAllowlistUnlistedAddressIsDeniedOnGetAsync()
    {
        var inner = new CountingKeyValueStore();
        await inner.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
        var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = OtherAddress };
        var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
            new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Read });

        await AssertAccessDeniedAsync(
            async () => _ = await store.GetAsync("shared:key"),
            $"Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have read permission.");
    }

    [Test]
    public async Task AclAllowlistUnlistedAddressIsDeniedOnPutAsync()
    {
        var inner = new CountingKeyValueStore();
        var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = OtherAddress };
        var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
            new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Write });

        await AssertAccessDeniedAsync(
            () => store.PutAsync("shared:key", Encoding.UTF8.GetBytes("value")),
            $"Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have write permission.");
    }

    [Test]
    public async Task AclAllowlistAdminGrantsReadAndWriteAsync()
    {
        var inner = new CountingKeyValueStore();
        var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress };
        var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
            new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Admin });

        await store.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
        var value = await store.GetAsync("shared:key");
        AssertEqual("value", Encoding.UTF8.GetString(value!));
        Assert(await store.DeleteAsync("shared:key"), "Admin permission should allow delete.");
    }

    [Test]
    public async Task AclDenylistBlockedAddressIsDeniedAndNonBlockedAddressIsAllowedAsync()
    {
        var inner = new CountingKeyValueStore();
        await inner.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
        var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = BlockedAddress };
        var store = CreateAclStore(inner, accessor, true, AclMode.Denylist,
            new AclEntry { EthAddress = BlockedAddress, Permission = AclPermission.Admin });

        await AssertAccessDeniedAsync(
            async () => _ = await store.GetAsync("shared:key"),
            $"Access denied: address {EthereumAddress.Normalize(BlockedAddress)} does not have read permission.");

        accessor.CurrentAddress = AllowedAddress;
        var value = await store.GetAsync("shared:key");
        AssertEqual("value", Encoding.UTF8.GetString(value!));
    }

    [Test]
    public async Task AclDisabledPassesAllOperationsThroughAsync()
    {
        var inner = new CountingKeyValueStore();
        var store = CreateAclStore(inner, enabled: false);

        await store.PutAsync("open:key", Encoding.UTF8.GetBytes("value"));
        AssertEqual("value", Encoding.UTF8.GetString((await store.GetAsync("open:key"))!));
        Assert(await store.SetTtlAsync("open:key", TimeSpan.FromMinutes(1)), "Disabled ACL should not block TTL updates.");
        Assert(await store.DeleteAsync("open:key"), "Disabled ACL should not block deletes.");
    }

    [Test]
    public Task AclStartupFailsWhenEnabledWithEmptyEntriesAsync()
    {
        var inner = new CountingKeyValueStore();
        var options = Options.Create(new AclOptions { Enabled = true, Mode = AclMode.Allowlist, Entries = [] });

        var threw = false;
        try
        {
            _ = new AclKeyValueStore(inner, options, new AsyncLocalEthAddressAccessor());
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("SWARM_KEYDB_ACL_ENTRIES", StringComparison.Ordinal))
        {
            threw = true;
        }

        Assert(threw, "Constructor must throw InvalidOperationException when ACL is enabled but no entries are configured.");
        return Task.CompletedTask;
    }

    [Test]
    public async Task ServiceCollectionPlacesAclBetweenSwarmAndEncryptionAsync()
    {
        var services = new ServiceCollection();
        services.AddOptions();
        services.AddSingleton<IOptions<CacheOptions>>(Options.Create(new CacheOptions { Enabled = true, MaxEntries = 8 }));
        services.AddSingleton<IOptions<CompressionOptions>>(Options.Create(new CompressionOptions { Enabled = true, MinSizeBytes = 0 }));
        services.AddSingleton<IOptions<EncryptionOptions>>(Options.Create(new EncryptionOptions { Enabled = true, KeyHex = MakeKeyHex() }));
        services.AddSingleton<IOptions<AclOptions>>(Options.Create(new AclOptions
        {
            Enabled = true,
            Mode = AclMode.Allowlist,
            Entries = [new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Admin }]
        }));
        services.AddSingleton<IEthAddressAccessor>(new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress });
        services.AddSingleton<Microsoft.Extensions.Caching.Memory.IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
        services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CachingKeyValueStore>>(NullLogger<CachingKeyValueStore>.Instance);
        services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CompressingKeyValueStore>>(NullLogger<CompressingKeyValueStore>.Instance);
        services.AddSingleton<Microsoft.Extensions.Logging.ILogger<EncryptingKeyValueStore>>(NullLogger<EncryptingKeyValueStore>.Instance);
        services.AddSwarmKeyDbStore(new InMemorySwarmClient(), new InMemoryKeyIndex());

        using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IKeyValueStore>();

        AssertEqual(typeof(CachingKeyValueStore), store.GetType());
        AssertEqual(typeof(CrdtKeyValueStore), GetInnerStoreAtDepth(store, depth: 1).GetType());
        AssertEqual(typeof(CompressingKeyValueStore), GetInnerStoreAtDepth(store, depth: 2).GetType());
        AssertEqual(typeof(EncryptingKeyValueStore), GetInnerStoreAtDepth(store, depth: 3).GetType());
        AssertEqual(typeof(AclKeyValueStore), GetInnerStoreAtDepth(store, depth: 4).GetType());
        AssertEqual(typeof(SwarmKeyValueStore), GetInnerStoreAtDepth(store, depth: 5).GetType());

        await store.PutAsync("pipeline:key", Encoding.UTF8.GetBytes("value"));
        AssertEqual("value", Encoding.UTF8.GetString((await store.GetAsync("pipeline:key"))!));
    }

    [Test]
    public async Task CachedReadStillRequiresAclPermissionAsync()
    {
        var services = new ServiceCollection();
        services.AddOptions();
        services.AddSingleton<IOptions<CacheOptions>>(Options.Create(new CacheOptions { Enabled = true, MaxEntries = 8 }));
        services.AddSingleton<IOptions<CompressionOptions>>(Options.Create(new CompressionOptions { Enabled = true, MinSizeBytes = 0 }));
        services.AddSingleton<IOptions<EncryptionOptions>>(Options.Create(new EncryptionOptions { Enabled = true, KeyHex = MakeKeyHex() }));
        services.AddSingleton<IOptions<AclOptions>>(Options.Create(new AclOptions
        {
            Enabled = true,
            Mode = AclMode.Allowlist,
            Entries = [new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Admin }]
        }));
        var accessor = new AsyncLocalEthAddressAccessor();
        services.AddSingleton<IEthAddressAccessor>(accessor);
        services.AddSingleton<Microsoft.Extensions.Caching.Memory.IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
        services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CachingKeyValueStore>>(NullLogger<CachingKeyValueStore>.Instance);
        services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CompressingKeyValueStore>>(NullLogger<CompressingKeyValueStore>.Instance);
        services.AddSingleton<Microsoft.Extensions.Logging.ILogger<EncryptingKeyValueStore>>(NullLogger<EncryptingKeyValueStore>.Instance);
        services.AddSwarmKeyDbStore(new InMemorySwarmClient(), new InMemoryKeyIndex());

        using var provider = services.BuildServiceProvider();
        var processor = new RedisCommandProcessor(provider.GetRequiredService<IKeyValueStore>(), accessor);

        var allowed = await ExecuteAsync(processor,
            RespCommand("AUTHADDR", AllowedAddress) +
            RespCommand("SET", "shared:key", "value") +
            RespCommand("GET", "shared:key"));
        AssertEqual("+OK\r\n+OK\r\n$5\r\nvalue\r\n", allowed);

        var denied = await ExecuteAsync(processor,
            RespCommand("AUTHADDR", OtherAddress) +
            RespCommand("GET", "shared:key"));
        AssertEqual(
            $"+OK\r\n-ERR Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have read permission.\r\n",
            denied);
    }

    [Test]
    public async Task RedisProtocolReturnsAccessDeniedErrorForUnauthorizedAddressAsync()
    {
        var accessor = new AsyncLocalEthAddressAccessor();
        var store = CreateAclStore(new CountingKeyValueStore(), accessor, true, AclMode.Allowlist,
            new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Read });
        var processor = new RedisCommandProcessor(store, accessor);

        var response = await ExecuteAsync(processor,
            RespCommand("AUTHADDR", OtherAddress) +
            RespCommand("GET", "shared:key"));

        AssertEqual(
            $"+OK\r\n-ERR Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have read permission.\r\n",
            response);
    }

    [Test]
    public Task CompositeKeyConstructsKeyFromSegmentsAsync()
    {
        AssertEqual("a:b:c", CompositeKey.Of("a", "b", "c"));
        AssertEqual("users:alice:profile", CompositeKey.Of("users", "alice", "profile"));
        AssertEqual("single", CompositeKey.Of("single"));
        return Task.CompletedTask;
    }

    [Test]
    public Task CompositeKeyRejectsSegmentContainingSeparatorAsync()
    {
        var threw = false;
        try
        {
            _ = CompositeKey.Of("a", "b:c");
        }
        catch (ArgumentException ex) when (ex.Message.Contains("separator", StringComparison.Ordinal))
        {
            threw = true;
        }

        Assert(threw, "Expected ArgumentException when a segment contains the separator character.");
        return Task.CompletedTask;
    }

    [Test]
    public Task CompositeKeyRejectsEmptySegmentAsync()
    {
        var threw = false;
        try
        {
            _ = CompositeKey.Of("a", "", "c");
        }
        catch (ArgumentException)
        {
            threw = true;
        }

        Assert(threw, "Expected ArgumentException when a segment is empty.");

        // Also verify null segment throws
        threw = false;
        try
        {
            _ = CompositeKey.Of("a", null!, "c");
        }
        catch (ArgumentException)
        {
            threw = true;
        }

        Assert(threw, "Expected ArgumentException when a segment is null.");
        return Task.CompletedTask;
    }

    [Test]
    public Task CompositeKeySupportsCustomSeparatorAsync()
    {
        AssertEqual("users/alice/profile", CompositeKey.Of('/', "users", "alice", "profile"));

        var threw = false;
        try
        {
            _ = CompositeKey.Of('/', "users/alice", "profile");
        }
        catch (ArgumentException ex) when (ex.Message.Contains("separator", StringComparison.Ordinal))
        {
            threw = true;
        }

        Assert(threw, "Expected ArgumentException when segment contains custom separator.");
        return Task.CompletedTask;
    }

    [Test]
    public async Task NamespacedStoreScopesPutAndGetToPrefixAsync()
    {
        var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var ns = new NamespacedKeyValueStore(inner, "users:alice:");

        await ns.PutAsync("profile", Encoding.UTF8.GetBytes("Alice"));
        var value = await ns.GetAsync("profile");
        AssertEqual("Alice", Encoding.UTF8.GetString(value!));

        // Verify underlying store has prefixed key
        var rawValue = await inner.GetAsync("users:alice:profile");
        AssertEqual("Alice", Encoding.UTF8.GetString(rawValue!));

        // Key without prefix should not be visible through namespaced view
        var notFound = await ns.GetAsync("users:alice:profile");
        Assert(notFound is null, "Namespaced store should not find a key that includes the prefix in its name.");
    }

    [Test]
    public async Task NamespacedStoreListKeysStripsPrefixAsync()
    {
        var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        await inner.PutAsync("users:alice:profile", Encoding.UTF8.GetBytes("1"));
        await inner.PutAsync("users:alice:settings", Encoding.UTF8.GetBytes("2"));
        await inner.PutAsync("users:bob:profile", Encoding.UTF8.GetBytes("3"));

        var ns = new NamespacedKeyValueStore(inner, "users:alice:");
        var keys = await ns.ListKeysAsync();

        AssertSequenceEqual(new[] { "profile", "settings" }, keys);
    }

    [Test]
    public async Task NamespacedStoreDeleteRemovesPrefixedKeyAsync()
    {
        var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var ns = new NamespacedKeyValueStore(inner, "ns:");

        await ns.PutAsync("key1", Encoding.UTF8.GetBytes("v1"));
        Assert(await ns.DeleteAsync("key1"), "Delete should return true for existing key.");
        Assert(await ns.GetAsync("key1") is null, "Key should be gone after delete.");

        // Underlying store should also have no prefixed key
        Assert(await inner.GetAsync("ns:key1") is null, "Underlying prefixed key should also be gone.");
    }

    [Test]
    public async Task NamespacedStoreIsolatesTwoNamespacesAsync()
    {
        var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var ns1 = new NamespacedKeyValueStore(inner, "ns1:");
        var ns2 = new NamespacedKeyValueStore(inner, "ns2:");

        await ns1.PutAsync("shared", Encoding.UTF8.GetBytes("from-ns1"));
        await ns2.PutAsync("shared", Encoding.UTF8.GetBytes("from-ns2"));

        AssertEqual("from-ns1", Encoding.UTF8.GetString((await ns1.GetAsync("shared"))!));
        AssertEqual("from-ns2", Encoding.UTF8.GetString((await ns2.GetAsync("shared"))!));

        var keys1 = await ns1.ListKeysAsync();
        var keys2 = await ns2.ListKeysAsync();
        AssertSequenceEqual(new[] { "shared" }, keys1);
        AssertSequenceEqual(new[] { "shared" }, keys2);
    }

    [Test]
    public async Task DeleteNamespaceRemovesAllKeysUnderPrefixAsync()
    {
        IKeyValueStore store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        await store.PutAsync("users:alice:profile", Encoding.UTF8.GetBytes("1"));
        await store.PutAsync("users:alice:settings", Encoding.UTF8.GetBytes("2"));
        await store.PutAsync("users:bob:profile", Encoding.UTF8.GetBytes("3"));

        var deleted = await store.DeleteNamespaceAsync("users:alice:");
        AssertEqual(2, deleted);

        var remaining = await store.ListKeysAsync();
        AssertSequenceEqual(new[] { "users:bob:profile" }, remaining);
    }

    [Test]
    public async Task WithNamespaceScopesClientOperationsAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var client = new SwarmKeyDbClient(store);

        var aliceDb = client.WithNamespace("users:alice:");
        await aliceDb.PutStringAsync("profile", "{\"name\":\"Alice\"}");
        await aliceDb.PutStringAsync("settings", "{\"theme\":\"dark\"}");

        var bobDb = client.WithNamespace("users:bob:");
        await bobDb.PutStringAsync("profile", "{\"name\":\"Bob\"}");

        // Scoped list returns only local keys
        var aliceKeys = await aliceDb.KeysAsync();
        AssertSequenceEqual(new[] { "profile", "settings" }, aliceKeys);

        var bobKeys = await bobDb.KeysAsync();
        AssertSequenceEqual(new[] { "profile" }, bobKeys);

        // Scoped get returns correct value
        AssertEqual("{\"name\":\"Alice\"}", await aliceDb.GetStringAsync("profile"));
        AssertEqual("{\"name\":\"Bob\"}", await bobDb.GetStringAsync("profile"));

        // Delete namespace via client
        var deletedCount = await client.DeleteNamespaceAsync("users:alice:");
        AssertEqual(2, deletedCount);

        var afterDelete = await aliceDb.KeysAsync();
        AssertSequenceEqual(Array.Empty<string>(), afterDelete);

        // Bob's data unaffected
        AssertEqual("{\"name\":\"Bob\"}", await bobDb.GetStringAsync("profile"));
    }

    [Test]
    public async Task BTreeIndexLookupInsertDeleteAsync()
    {
        var index = new BTreeKeyIndex();

        // Insert and lookup
        await index.SetReferenceAsync("b", "ref-b");
        await index.SetReferenceAsync("a", "ref-a");
        await index.SetReferenceAsync("c", "ref-c");

        AssertEqual("ref-a", await index.GetReferenceAsync("a"));
        AssertEqual("ref-b", await index.GetReferenceAsync("b"));
        AssertEqual("ref-c", await index.GetReferenceAsync("c"));
        AssertEqual(null, await index.GetReferenceAsync("missing"));

        // Keys are returned in sorted order
        AssertSequenceEqual(new[] { "a", "b", "c" }, await index.ListKeysAsync());

        // Delete
        Assert(await index.RemoveAsync("b"), "Delete should return true for existing key.");
        Assert(!await index.RemoveAsync("b"), "Delete should return false for missing key.");
        AssertEqual(null, await index.GetReferenceAsync("b"));
        AssertSequenceEqual(new[] { "a", "c" }, await index.ListKeysAsync());
    }

    [Test]
    public async Task BTreeIndexRangeScanReturnsOrderedSubsetAsync()
    {
        var index = new BTreeKeyIndex();
        foreach (var k in new[] { "k:1", "k:2", "k:3", "k:4", "k:5" })
        {
            await index.SetReferenceAsync(k, "ref");
        }

        // Inclusive bounds
        var range = await index.GetKeysInRangeAsync("k:2", "k:4", includeStart: true, includeEnd: true);
        AssertSequenceEqual(new[] { "k:2", "k:3", "k:4" }, range);

        // Exclusive start
        var excStart = await index.GetKeysInRangeAsync("k:2", "k:4", includeStart: false, includeEnd: true);
        AssertSequenceEqual(new[] { "k:3", "k:4" }, excStart);

        // Exclusive end
        var excEnd = await index.GetKeysInRangeAsync("k:2", "k:4", includeStart: true, includeEnd: false);
        AssertSequenceEqual(new[] { "k:2", "k:3" }, excEnd);

        // Open lower bound
        var openLow = await index.GetKeysInRangeAsync(null, "k:2", includeEnd: true);
        AssertSequenceEqual(new[] { "k:1", "k:2" }, openLow);

        // Open upper bound
        var openHigh = await index.GetKeysInRangeAsync("k:4", null, includeStart: true);
        AssertSequenceEqual(new[] { "k:4", "k:5" }, openHigh);
    }

    [Test]
    public async Task BTreeIndexPrefixScanUsesEfficientRangeAsync()
    {
        var index = new BTreeKeyIndex();
        await index.SetReferenceAsync("user:alice:profile", "r1");
        await index.SetReferenceAsync("user:alice:settings", "r2");
        await index.SetReferenceAsync("user:bob:profile", "r3");
        await index.SetReferenceAsync("zzz:other", "r4");

        // Prefix range: ["user:alice:", "user:alice;") — ';' is the char after ':'
        var aliceKeys = await index.GetKeysInRangeAsync("user:alice:", "user:alice;", includeStart: true, includeEnd: false);
        AssertSequenceEqual(new[] { "user:alice:profile", "user:alice:settings" }, aliceKeys);
    }

    [Test]
    public async Task BTreeIndexExpiryEvictsKeysOnAccessAsync()
    {
        var index = new BTreeKeyIndex();
        var past = DateTimeOffset.UtcNow.AddSeconds(-1);
        await index.SetReferenceAsync("expired", "ref", past);
        await index.SetReferenceAsync("alive", "ref-alive");

        AssertEqual(null, await index.GetReferenceAsync("expired"));
        AssertEqual("ref-alive", await index.GetReferenceAsync("alive"));
        AssertSequenceEqual(new[] { "alive" }, await index.ListKeysAsync());
    }

    [Test]
    public async Task BTreeIndexRebuildPurgesExpiredEntriesAsync()
    {
        var index = new BTreeKeyIndex();
        var past = DateTimeOffset.UtcNow.AddSeconds(-1);
        await index.SetReferenceAsync("gone", "ref", past);
        await index.SetReferenceAsync("keep", "ref2");

        await index.RebuildIndexAsync();

        AssertSequenceEqual(new[] { "keep" }, await index.ListKeysAsync());
    }

    [Test]
    public async Task BTreeIndexRangeScanOpenBoundsAsync()
    {
        var index = new BTreeKeyIndex();
        foreach (var k in new[] { "aaa", "bbb", "ccc" })
        {
            await index.SetReferenceAsync(k, "r");
        }

        // Fully open range returns all keys
        var all = await index.GetKeysInRangeAsync(null, null);
        AssertSequenceEqual(new[] { "aaa", "bbb", "ccc" }, all);
    }

    [Test]
    public async Task SwarmStoreWithBTreeIndexRangeScanAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new BTreeKeyIndex());
        await store.PutAsync("order:001", Encoding.UTF8.GetBytes("first"));
        await store.PutAsync("order:002", Encoding.UTF8.GetBytes("second"));
        await store.PutAsync("order:003", Encoding.UTF8.GetBytes("third"));
        await store.PutAsync("other:x", Encoding.UTF8.GetBytes("outside"));

        var range = await store.GetKeyRangeAsync("order:001", "order:002", new RangeScanOptions
        {
            IncludeStart = true,
            IncludeEnd = true,
            IncludeValues = true
        });

        AssertSequenceEqual(new[] { "order:001", "order:002" }, range.Select(e => e.Key));
        AssertEqual("first", Encoding.UTF8.GetString(range[0].Value!));
        AssertEqual("second", Encoding.UTF8.GetString(range[1].Value!));
    }

    [Test]
    public async Task SwarmStoreWithBTreeIndexPrefixScanAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new BTreeKeyIndex());
        await store.PutAsync("tag:alpha", Encoding.UTF8.GetBytes("1"));
        await store.PutAsync("tag:beta", Encoding.UTF8.GetBytes("2"));
        await store.PutAsync("other:gamma", Encoding.UTF8.GetBytes("3"));

        var keys = await store.GetKeysWithPrefixAsync("tag:");
        AssertSequenceEqual(new[] { "tag:alpha", "tag:beta" }, keys);
    }

}
