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
public class CoreStoreTests
{
    [Test]
    public async Task ClientStoresSupportedValuesAsync()
    {
        var client = new SwarmKeyDbClient(new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));

        await client.PutStringAsync("profile:name", "Ada");
        await client.PutJsonAsync("profile:settings", new Settings(true, 3));
        await client.PutBytesAsync("profile:avatar", new byte[] { 0, 1, 2, 255 });

        AssertEqual("Ada", await client.GetStringAsync("profile:name"));
        var settings = await client.GetJsonAsync<Settings>("profile:settings");
        Assert(settings is { Enabled: true, Count: 3 }, "JSON value should round-trip.");
        AssertSequenceEqual(new byte[] { 0, 1, 2, 255 }, await client.GetBytesAsync("profile:avatar") ?? Array.Empty<byte>());
        AssertSequenceEqual(new[] { "profile:avatar", "profile:name", "profile:settings" }, await client.KeysAsync());
        Assert(await client.DeleteAsync("profile:name"), "Delete should report existing key.");
        AssertEqual(null, await client.GetStringAsync("profile:name"));
    }

    [Test]
    public async Task SwarmStoreWritesIntegrityEnvelopeAsync()
    {
        var swarm = new MutableSwarmClient();
        var index = new InMemoryKeyIndex();
        var store = new SwarmKeyValueStore(swarm, index);

        var value = Encoding.UTF8.GetBytes("{\"name\":\"Alice\"}");
        var expectedHash = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(value));
        await store.PutAsync("user:alice", value);

        var reference = await index.GetReferenceAsync("user:alice");
        Assert(reference is not null, "Expected reference to be written for integrity-protected key.");

        var raw = await swarm.DownloadAsync(reference!);
        Assert(raw.Take(4).SequenceEqual(new byte[] { 0x53, 0x4B, 0x49, 0x31 }), "Stored payload should start with the integrity envelope magic header.");

        using var envelope = JsonDocument.Parse(raw.AsMemory(4));
        AssertEqual(1, envelope.RootElement.GetProperty("version").GetInt32());
        AssertEqual("SHA-256", envelope.RootElement.GetProperty("hashAlgorithm").GetString());
        AssertEqual(expectedHash, envelope.RootElement.GetProperty("hash").GetString());
        AssertSequenceEqual(value, envelope.RootElement.GetProperty("payload").GetBytesFromBase64());
    }

    [Test]
    public async Task SwarmStoreDetectsTamperedValueAsync()
    {
        var swarm = new MutableSwarmClient();
        var index = new InMemoryKeyIndex();
        var store = new SwarmKeyValueStore(swarm, index);

        await store.PutAsync("profile:1", Encoding.UTF8.GetBytes("original"));
        var reference = await index.GetReferenceAsync("profile:1");
        Assert(reference is not null, "Expected reference for tamper test.");

        swarm.Corrupt(reference!, raw =>
        {
            using var document = JsonDocument.Parse(raw.AsMemory(4));
            var payload = document.RootElement.GetProperty("payload").GetBytesFromBase64();
            payload[0] ^= 0x01;
            var tamperedEnvelope = new
            {
                version = document.RootElement.GetProperty("version").GetInt32(),
                hashAlgorithm = document.RootElement.GetProperty("hashAlgorithm").GetString(),
                hash = document.RootElement.GetProperty("hash").GetString(),
                payload
            };
            var json = JsonSerializer.SerializeToUtf8Bytes(tamperedEnvelope);
            var mutated = new byte[4 + json.Length];
            Buffer.BlockCopy(raw, 0, mutated, 0, 4);
            Buffer.BlockCopy(json, 0, mutated, 4, json.Length);
            return mutated;
        });

        var threw = false;
        try
        {
            _ = await store.GetAsync("profile:1");
        }
        catch (DataIntegrityException ex) when (
            ex.KeyName == "profile:1" &&
            ex.ExpectedHash is not null &&
            ex.ActualHash is not null &&
            ex.Message.Contains("Data integrity check failed for key 'profile:1'.", StringComparison.Ordinal))
        {
            threw = true;
        }

        Assert(threw, "Expected DataIntegrityException when the stored payload hash no longer matches.");
    }

    [Test]
    public async Task SwarmStoreCanDisableIntegrityVerificationAsync()
    {
        var swarm = new MutableSwarmClient();
        var index = new InMemoryKeyIndex();
        var enabledStore = new SwarmKeyValueStore(swarm, index);
        var disabledStore = new SwarmKeyValueStore(swarm, index, new IntegrityOptions { Enabled = false });

        var value = Encoding.UTF8.GetBytes("plain");
        await enabledStore.PutAsync("wrapped", value);
        AssertSequenceEqual(value, (await disabledStore.GetAsync("wrapped"))!);

        await disabledStore.PutAsync("raw", value);
        var rawReference = await index.GetReferenceAsync("raw");
        Assert(rawReference is not null, "Expected reference for raw integrity-disabled write.");
        AssertSequenceEqual(value, await swarm.DownloadAsync(rawReference!));
    }

    [Test]
    public async Task BatchGetDetectsTamperedKeyAsync()
    {
        var swarm = new MutableSwarmClient();
        var index = new InMemoryKeyIndex();
        var client = new SwarmKeyDbClient(new SwarmKeyValueStore(swarm, index));

        await client.BatchPutAsync(
        [
            new KeyValuePair<string, ReadOnlyMemory<byte>>("safe", Encoding.UTF8.GetBytes("1")),
            new KeyValuePair<string, ReadOnlyMemory<byte>>("tampered", Encoding.UTF8.GetBytes("2"))
        ]);

        var reference = await index.GetReferenceAsync("tampered");
        Assert(reference is not null, "Expected reference for tampered batch key.");
        swarm.Corrupt(reference!, raw =>
        {
            var mutated = raw.ToArray();
            mutated[^1] ^= 0x01;
            return mutated;
        });

        var threw = false;
        try
        {
            _ = await client.BatchGetAsync(["safe", "tampered"]);
        }
        catch (DataIntegrityException ex) when (ex.KeyName == "tampered")
        {
            threw = true;
        }

        Assert(threw, "Expected batch get to fail with DataIntegrityException for the corrupted key.");
    }

    [Test]
    public async Task SwarmStoreIntegritySupportsEmptyAndLargeValuesAsync()
    {
        const int BytePatternModulus = 251;
        var swarm = new MutableSwarmClient();
        var store = new SwarmKeyValueStore(swarm, new InMemoryKeyIndex());

        await store.PutAsync("empty", Array.Empty<byte>());
        AssertSequenceEqual(Array.Empty<byte>(), (await store.GetAsync("empty"))!);

        var large = Enumerable.Range(0, 1_100_000).Select(i => (byte)(i % BytePatternModulus)).ToArray();
        await store.PutAsync("large", large);
        AssertSequenceEqual(large, (await store.GetAsync("large"))!);
    }

    [Test]
    public async Task BeeClientParsesUploadReferenceAsync()
    {
        var handler = new StubHttpMessageHandler(request =>
        {
            AssertEqual("POST", request.Method.Method);
            Assert(request.Headers.Contains("swarm-postage-batch-id"), "Upload should include postage batch header.");
            return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent("{\"reference\":\"abc123\"}")
            };
        });
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://localhost:1633/") };
        var client = new BeeSwarmClient(httpClient, "batch-id");

        AssertEqual("abc123", await client.UploadAsync(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task BeeClientUploadAndDownloadRoundTripAsync()
    {
        var payload = Encoding.UTF8.GetBytes("bee-payload");
        var handler = new StubHttpMessageHandler(request =>
        {
            if (request.Method == HttpMethod.Post && request.RequestUri?.AbsolutePath == "/bytes")
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{\"reference\":\"abc123\"}")
                };
            }

            if (request.Method == HttpMethod.Get && request.RequestUri?.AbsolutePath == "/bytes/abc123")
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(payload)
                };
            }

            return new HttpResponseMessage(HttpStatusCode.NotFound);
        });

        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://bee.local/") };
        var client = new BeeSwarmClient(httpClient, "NULL_STAMP");

        var reference = await client.UploadAsync(payload);
        AssertEqual("abc123", reference);
        AssertSequenceEqual(payload, await client.DownloadAsync(reference));
    }

    [Test]
    public void BeeClientUploadThrowsOnHttpFailureAsync()
    {
        var handler = new StubHttpMessageHandler(_ => new HttpResponseMessage(HttpStatusCode.NotFound));
        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://bee.local/") };
        var client = new BeeSwarmClient(httpClient, "NULL_STAMP");

        NUnit.Framework.Assert.ThrowsAsync<HttpRequestException>(async () => await client.UploadAsync(Encoding.UTF8.GetBytes("v")));
    }

    [Test]
    public void BeeClientRejectsReadGatewayAsUploadEndpoint()
    {
        var ex = NUnit.Framework.Assert.Throws<InvalidOperationException>(
            () => new BeeSwarmClient(new Uri("https://bzz.limo"), "batch-id"));

        Assert(ex is not null && ex.Message.Contains("read gateway", StringComparison.OrdinalIgnoreCase),
            "Expected a clear error for bzz.limo upload misconfiguration.");
    }

    [Test]
    public async Task BeeConsistencyVerifierValidatesFeedRevisionAsync()
    {
        var handler = new StubHttpMessageHandler(request =>
        {
            AssertEqual("/feeds/1111111111111111111111111111111111111111/aabbcc", request.RequestUri!.AbsolutePath);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"feedIndex\":\"0x2a\"}")
            };
        });
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://bee.local/") };
        var verifier = new BeeConsistencyVerifier(http, new ConsistencyOptions
        {
            FeedOwner = "1111111111111111111111111111111111111111"
        });

        var result = await verifier.VerifyFeedRevisionAsync("aabbcc", 42, CancellationToken.None);
        Assert(result.IsValid, "Expected feed revision verification to pass.");
    }

    [Test]
    public async Task BeeConsistencyVerifierDetectsHashMismatchAsync()
    {
        var payload = Encoding.UTF8.GetBytes("actual");
        var expected = System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes("expected"));
        var handler = new StubHttpMessageHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(payload)
        });
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://bee.local/") };
        var verifier = new BeeConsistencyVerifier(http, new ConsistencyOptions());

        var result = await verifier.VerifyContentHashAsync("abc", expected, CancellationToken.None);
        Assert(!result.IsValid, "Expected content hash verification to fail for mismatched payload.");
        Assert(result.FailureReason.Contains("mismatch", StringComparison.OrdinalIgnoreCase), "Expected mismatch reason.");
    }

    [Test]
    public async Task QuorumConsistencyVerifierRequiresThresholdAsync()
    {
        var quorum = new QuorumConsistencyVerifier(
        [
            new StaticConsistencyVerifier(VerificationResult.Passed("content-hash", "node-1", TimeSpan.FromMilliseconds(3), "a", "a")),
            new StaticConsistencyVerifier(VerificationResult.Failed("content-hash", "node-2", TimeSpan.FromMilliseconds(5), "a", "b", "mismatch")),
            new StaticConsistencyVerifier(VerificationResult.Failed("content-hash", "node-3", TimeSpan.FromMilliseconds(4), "a", "b", "mismatch"))
        ],
            threshold: 2);

        var threw = false;
        try
        {
            _ = await quorum.VerifyContentHashAsync("ref", [1], CancellationToken.None);
        }
        catch (QuorumNotMetException ex)
        {
            threw = true;
            AssertEqual(2, ex.Threshold);
            AssertEqual(1, ex.Succeeded);
        }

        Assert(threw, "Expected quorum verifier to throw when threshold is not met.");
    }

    [Test]
    public async Task ConsistencyMiddlewareStrictModeThrowsOnViolationAsync()
    {
        var inner = new MetadataCountingStore();
        await inner.PutAsync("profile:name", Encoding.UTF8.GetBytes("Ada"));
        await inner.SetReferenceAsync("profile:name", "ref-1");
        var verifier = new StaticConsistencyVerifier(VerificationResult.Failed("content-hash", "http://bee.local", TimeSpan.FromMilliseconds(4), "expected", "actual", "hash mismatch"));
        var logger = new TestLogger<ConsistencyVerificationMiddleware>();
        var middleware = new ConsistencyVerificationMiddleware(
            inner,
            verifier,
            Options.Create(new ConsistencyOptions { Enabled = true, FailureMode = ConsistencyFailureMode.Strict }),
            logger);

        var threw = false;
        try
        {
            _ = await middleware.GetAsync("profile:name");
        }
        catch (ConsistencyViolationException ex)
        {
            threw = true;
            AssertEqual("profile:name", ex.Key);
        }

        Assert(threw, "Expected strict consistency mode to throw on verification failure.");
    }

    [Test]
    public async Task ConsistencyMiddlewareWarnModeLogsAndReturnsValueAsync()
    {
        var inner = new MetadataCountingStore();
        await inner.PutAsync("profile:name", Encoding.UTF8.GetBytes("Ada"));
        await inner.SetReferenceAsync("profile:name", "ref-1");
        var verifier = new StaticConsistencyVerifier(VerificationResult.Failed("content-hash", "http://bee.local", TimeSpan.FromMilliseconds(4), "expected", "actual", "hash mismatch"));
        var logger = new TestLogger<ConsistencyVerificationMiddleware>();
        var middleware = new ConsistencyVerificationMiddleware(
            inner,
            verifier,
            Options.Create(new ConsistencyOptions { Enabled = true, FailureMode = ConsistencyFailureMode.Warn }),
            logger);

        var value = await middleware.GetAsync("profile:name");
        AssertEqual("Ada", Encoding.UTF8.GetString(value!));
        Assert(logger.Messages.Any(message => message.Contains("Consistency verification failed", StringComparison.Ordinal)), "Expected warning log in warn mode.");
    }

    [Test]
    public async Task ConsistencyMiddlewareWarnModeEvictsCacheEntryOnFailureAsync()
    {
        // Set up an in-memory store + caching layer so we can verify eviction behaviour.
        var baseStore = new MetadataCountingStore();
        await baseStore.PutAsync("cache:key", Encoding.UTF8.GetBytes("swarm-value"));
        await baseStore.SetReferenceAsync("cache:key", "ref-evict");

        var cache = new MemoryCache(new MemoryCacheOptions());
        var cachingStore = new CachingKeyValueStore(
            baseStore,
            cache,
            Options.Create(new CacheOptions { Enabled = true }),
            NullLogger<CachingKeyValueStore>.Instance);

        // Prime the cache by doing a read.
        await cachingStore.GetAsync("cache:key");
        AssertEqual(1, cachingStore.Hits + cachingStore.Misses);
        AssertEqual(0, cachingStore.Hits); // first read is always a miss

        // Do a second read — should be a cache hit now.
        _ = await cachingStore.GetAsync("cache:key");
        AssertEqual(1, cachingStore.Hits);

        // Now wrap with the verification middleware (fail mode = Warn).
        var failingVerifier = new StaticConsistencyVerifier(VerificationResult.Failed("content-hash", "http://bee.local", TimeSpan.FromMilliseconds(5), "expected", "actual", "hash mismatch"));
        var logger = new TestLogger<ConsistencyVerificationMiddleware>();
        var middleware = new ConsistencyVerificationMiddleware(
            cachingStore,
            failingVerifier,
            Options.Create(new ConsistencyOptions { Enabled = true, FailureMode = ConsistencyFailureMode.Warn }),
            logger);

        // Prime the cache again through the middleware.
        await middleware.GetAsync("cache:key");
        var hitsBefore = cachingStore.Hits;

        // Trigger a verification failure — the middleware should evict the cache entry.
        var evictionsBefore = cachingStore.Evictions;
        _ = await middleware.GetAsync("cache:key");
        var evictionsAfter = cachingStore.Evictions;

        Assert(evictionsAfter > evictionsBefore, "Expected cache evictions to increment after a verification failure.");
        var snapshot = middleware.GetSnapshot();
        Assert(snapshot.EvictionByVerificationTotal > 0, "Expected EvictionByVerificationTotal to be non-zero.");
        Assert(snapshot.ViolationCount > 0, "Expected violation count to be non-zero.");
    }

    [Test]
    public async Task ConsistencyMiddlewareWarnModeInvokesCallbackAsync()
    {
        var inner = new MetadataCountingStore();
        await inner.PutAsync("cb:key", Encoding.UTF8.GetBytes("value"));
        await inner.SetReferenceAsync("cb:key", "ref-cb");

        var callbackInvoked = false;
        string? callbackKey = null;
        VerificationResult? callbackResult = null;

        var verifier = new StaticConsistencyVerifier(VerificationResult.Failed("content-hash", "http://bee.local", TimeSpan.FromMilliseconds(3), "exp", "act", "mismatch"));
        var options = new ConsistencyOptions
        {
            Enabled = true,
            FailureMode = ConsistencyFailureMode.Warn,
            OnVerificationFailure = (key, result) =>
            {
                callbackInvoked = true;
                callbackKey = key;
                callbackResult = result;
            }
        };
        var middleware = new ConsistencyVerificationMiddleware(
            inner,
            verifier,
            Options.Create(options),
            NullLogger<ConsistencyVerificationMiddleware>.Instance);

        _ = await middleware.GetAsync("cb:key");

        Assert(callbackInvoked, "Expected OnVerificationFailure callback to be invoked.");
        AssertEqual("cb:key", callbackKey);
        Assert(callbackResult is not null, "Expected callback to receive a VerificationResult.");
        Assert(!callbackResult!.IsValid, "Expected callback to receive a failed result.");
    }

    [Test]
    public async Task ConsistencyMiddlewareEvictionCounterIncrementsAsync()
    {
        // Verify EvictionByVerificationTotal increments with a real CachingKeyValueStore.
        var baseStore = new MetadataCountingStore();
        await baseStore.PutAsync("evt:key", Encoding.UTF8.GetBytes("val"));
        await baseStore.SetReferenceAsync("evt:key", "ref-evt");

        var cachingStore = new CachingKeyValueStore(
            baseStore,
            new MemoryCache(new MemoryCacheOptions()),
            Options.Create(new CacheOptions { Enabled = true }),
            NullLogger<CachingKeyValueStore>.Instance);

        var failingVerifier = new StaticConsistencyVerifier(VerificationResult.Failed("content-hash", "http://bee.local", TimeSpan.FromMilliseconds(2), "exp", "act", "mismatch"));
        var middleware = new ConsistencyVerificationMiddleware(
            cachingStore,
            failingVerifier,
            Options.Create(new ConsistencyOptions { Enabled = true, FailureMode = ConsistencyFailureMode.Warn }),
            NullLogger<ConsistencyVerificationMiddleware>.Instance);

        var snapshot0 = middleware.GetSnapshot();
        AssertEqual(0, snapshot0.EvictionByVerificationTotal);

        _ = await middleware.GetAsync("evt:key");
        var snapshot1 = middleware.GetSnapshot();
        AssertEqual(1, snapshot1.EvictionByVerificationTotal);

        _ = await middleware.GetAsync("evt:key");
        var snapshot2 = middleware.GetSnapshot();
        AssertEqual(2, snapshot2.EvictionByVerificationTotal);
    }

    [Test]
    public async Task ConsistencyMiddlewarePassDoesNotEvictCacheAsync()
    {
        var baseStore = new MetadataCountingStore();
        await baseStore.PutAsync("pass:key", Encoding.UTF8.GetBytes("good-value"));
        await baseStore.SetReferenceAsync("pass:key", "ref-pass");

        var cachingStore = new CachingKeyValueStore(
            baseStore,
            new MemoryCache(new MemoryCacheOptions()),
            Options.Create(new CacheOptions { Enabled = true }),
            NullLogger<CachingKeyValueStore>.Instance);

        var passingVerifier = new StaticConsistencyVerifier(VerificationResult.Passed("content-hash", "http://bee.local", TimeSpan.FromMilliseconds(1), "a", "a"));
        var middleware = new ConsistencyVerificationMiddleware(
            cachingStore,
            passingVerifier,
            Options.Create(new ConsistencyOptions { Enabled = true }),
            NullLogger<ConsistencyVerificationMiddleware>.Instance);

        // Read twice; the second should be a cache hit with no eviction.
        await middleware.GetAsync("pass:key");
        await middleware.GetAsync("pass:key");

        var snapshot = middleware.GetSnapshot();
        AssertEqual(0, snapshot.EvictionByVerificationTotal);
        AssertEqual(2, snapshot.TotalVerifications);
        AssertEqual(0, snapshot.ViolationCount);
        AssertEqual(1, cachingStore.Hits); // second read was a cache hit
    }

    [Test]
    public async Task ConsistencyMiddlewareIntegrationEvictsCorruptedCacheAsync()
    {
        // Integration test: write a key, corrupt the cached value in-memory, then verify
        // that the next GetAsync evicts the corrupted entry and returns the Swarm value.
        var swarm = new InMemorySwarmClient();
        var index = new InMemoryKeyIndex();
        var baseStore = new SwarmKeyValueStore(swarm, index, new IntegrityOptions { Enabled = false });

        await baseStore.PutAsync("int:key", Encoding.UTF8.GetBytes("real-value"));

        var cache = new MemoryCache(new MemoryCacheOptions());
        var cachingStore = new CachingKeyValueStore(
            baseStore,
            cache,
            Options.Create(new CacheOptions { Enabled = true }),
            NullLogger<CachingKeyValueStore>.Instance);

        // Prime the cache.
        _ = await cachingStore.GetAsync("int:key");
        AssertEqual(0, cachingStore.Hits);

        // Inject a "corrupted" value directly into the cache to simulate divergence.
        var corruptEntry = cache.CreateEntry("int:key");
        corruptEntry.Value = Encoding.UTF8.GetBytes("corrupted-value");
        corruptEntry.Dispose();

        // Confirm the cache now serves the corrupted value.
        var cachedValue = await cachingStore.GetAsync("int:key");
        AssertEqual("corrupted-value", Encoding.UTF8.GetString(cachedValue!));
        AssertEqual(1, cachingStore.Hits);

        // Set up a verifier that always fails (simulating a Swarm mismatch).
        var failingVerifier = new StaticConsistencyVerifier(
            VerificationResult.Failed("content-hash", "http://bee.local", TimeSpan.FromMilliseconds(2), "exp", "act", "hash mismatch"));

        var middleware = new ConsistencyVerificationMiddleware(
            cachingStore,
            failingVerifier,
            Options.Create(new ConsistencyOptions { Enabled = true, FailureMode = ConsistencyFailureMode.Warn }),
            NullLogger<ConsistencyVerificationMiddleware>.Instance);

        // Reference is only available on SwarmKeyValueStore directly; expose it through the
        // cache so TryGetBackendReferenceAsync can find it.
        // SwarmKeyValueStore implements IBackendMetadataProvider; CachingKeyValueStore now
        // propagates it, so the middleware should find it through the chain.
        var reference = await ((IBackendMetadataProvider)cachingStore).GetBackendMetadataAsync("int:key");
        Assert(reference is not null, "Expected backend reference to be available through the CachingKeyValueStore.");

        var evictionsBefore = cachingStore.Evictions;

        // GetAsync through middleware: verification fails → evict → re-fetch from SwarmStore.
        var result = await middleware.GetAsync("int:key");

        var evictionsAfter = cachingStore.Evictions;
        Assert(evictionsAfter > evictionsBefore, "Expected cache eviction after verification failure.");
        AssertEqual("real-value", Encoding.UTF8.GetString(result!));
        var snapshot = middleware.GetSnapshot();
        Assert(snapshot.EvictionByVerificationTotal > 0, "Expected eviction counter to increment.");
    }

    [Test]
    public async Task IpfsBackendSupportsKeyValueOperationsAsync()
    {
        var catPayload = Encoding.UTF8.GetBytes("downloaded");
        var sawPinnedAddCall = false;
        var sawUnpinnedAddCall = false;
        var handler = new StubHttpMessageHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath.EndsWith("/api/v0/add", StringComparison.Ordinal) == true)
            {
                sawPinnedAddCall = request.RequestUri.Query.Contains("pin=true", StringComparison.Ordinal);
                sawUnpinnedAddCall = request.RequestUri.Query.Contains("pin=false", StringComparison.Ordinal);
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{\"Hash\":\"bafytestcid\"}")
                };
            }

            if (request.RequestUri?.AbsolutePath.EndsWith("/api/v0/cat", StringComparison.Ordinal) == true)
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(catPayload)
                };
            }

            return new HttpResponseMessage(HttpStatusCode.NotFound);
        });

        using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://localhost:5001/") };
        using var ipfsClient = new IpfsSwarmClient(httpClient, pinOnWrite: true);
        AssertEqual("bafytestcid", await ipfsClient.UploadAsync(Encoding.UTF8.GetBytes("upload")));
        Assert(sawPinnedAddCall, "IPFS uploads should set pin=true.");
        AssertSequenceEqual(catPayload, await ipfsClient.DownloadAsync("bafytestcid"));
        using var ipfsClientWithoutPinning = new IpfsSwarmClient(httpClient, pinOnWrite: false);
        AssertEqual("bafytestcid", await ipfsClientWithoutPinning.UploadAsync(Encoding.UTF8.GetBytes("upload")));
        Assert(sawUnpinnedAddCall, "IPFS uploads should set pin=false when pinning is disabled.");

        var store = new IpfsStorageBackend(new MutableSwarmClient(), new InMemoryKeyIndex());
        var processor = new RedisCommandProcessor(store);

        var response = await ExecuteAsync(processor,
            RespCommand("SET", "ipfs:key", "value") +
            RespCommand("GET", "ipfs:key") +
            RespCommand("KEYS", "*") +
            RespCommand("SCAN", "0", "COUNT", "10") +
            RespCommand("DEL", "ipfs:key") +
            RespCommand("GET", "ipfs:key"));

        Assert(response.Contains("+OK\r\n$5\r\nvalue\r\n", StringComparison.Ordinal), "IPFS backend should round-trip set/get.");
        Assert(response.Contains("*1\r\n$8\r\nipfs:key\r\n", StringComparison.Ordinal), "IPFS backend should list keys.");
        Assert(response.Contains(":1\r\n$-1\r\n", StringComparison.Ordinal), "IPFS backend should delete keys.");
    }

    [Test]
    public async Task HybridBackendFallsBackToAvailableStorageAsync()
    {
        var swarm = new MutableSwarmClient();
        var ipfs = new MutableSwarmClient();
        var index = new InMemoryKeyIndex();
        var store = new HybridBackend(swarm, ipfs, index);

        await store.PutAsync("hybrid:key", Encoding.UTF8.GetBytes("value"));
        var metadata = await ((IBackendMetadataProvider)store).GetBackendMetadataAsync("hybrid:key");
        Assert(metadata is not null, "Hybrid metadata should be available.");
        using var metadataDocument = JsonDocument.Parse(metadata!);
        Assert(!string.IsNullOrWhiteSpace(metadataDocument.RootElement.GetProperty("ipfsCid").GetString()), "Hybrid metadata should include IPFS CID.");
        Assert(!string.IsNullOrWhiteSpace(metadataDocument.RootElement.GetProperty("swarmReference").GetString()), "Hybrid metadata should include swarm reference.");

        var reference = await index.GetReferenceAsync("hybrid:key");
        if (reference is null || !HybridReferenceCodec.TryDecode(reference, out var decoded))
        {
            throw new Exception("Hybrid reference should be encoded.");
        }

        Assert(!string.IsNullOrWhiteSpace(decoded.SwarmReference), "Hybrid reference should include swarm hash.");
        Assert(!string.IsNullOrWhiteSpace(decoded.IpfsCid), "Hybrid reference should include IPFS CID.");

        swarm.Remove(decoded.SwarmReference!);
        AssertEqual("value", Encoding.UTF8.GetString((await store.GetAsync("hybrid:key"))!));

        ipfs.Remove(decoded.IpfsCid!);
        var threw = false;
        try
        {
            _ = await store.GetAsync("hybrid:key");
        }
        catch (KeyNotFoundException)
        {
            threw = true;
        }

        Assert(threw, "Expected error when all hybrid backends are unavailable.");
    }

    [Test]
    public async Task RedisBackendMetaCommandReturnsMetadataAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor = new RedisCommandProcessor(store);
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "meta:key", "1") +
            RespCommand("BACKENDMETA", "meta:key"));

        Assert(response.Contains("\"swarmReference\":", StringComparison.Ordinal), "BACKENDMETA should include backend metadata.");
    }

    [Test]
    public async Task RedisProtocolRoundTripAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "hello", "world") +
            RespCommand("GET", "hello") +
            RespCommand("EXISTS", "hello", "missing") +
            RespCommand("DEL", "hello") +
            RespCommand("GET", "hello"));

        AssertEqual("+OK\r\n$5\r\nworld\r\n:1\r\n:1\r\n$-1\r\n", response);
    }

    [Test]
    public async Task RedisProtocolKeyIterationAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "app:a", "1") +
            RespCommand("SET", "app:b", "2") +
            RespCommand("SET", "other", "3") +
            RespCommand("KEYS", "app:*") +
            RespCommand("SCAN", "0", "MATCH", "app:*", "COUNT", "1"));

        Assert(response.Contains("*2\r\n$5\r\napp:a\r\n$5\r\napp:b\r\n", StringComparison.Ordinal), "KEYS should return matching app keys.");
        Assert(response.EndsWith("*2\r\n$1\r\n1\r\n*1\r\n$5\r\napp:a\r\n", StringComparison.Ordinal), "SCAN should return next cursor and one key.");
    }

    [Test]
    public async Task RedisCompatibilityCommandsAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("INFO", "all") +
            RespCommand("COMMAND", "COUNT") +
            RespCommand("COMMAND", "INFO", "GET") +
            RespCommand("CONFIG", "GET", "maxmemory") +
            RespCommand("CONFIG", "SET", "maxmemory", "1mb") +
            RespCommand("CONFIG", "GET", "maxmemory") +
            RespCommand("CLIENT", "ID"));

        Assert(response.Contains("# Server", StringComparison.Ordinal), "INFO all should include server section.");
        Assert(response.Contains("get\r\n", StringComparison.Ordinal), "COMMAND results should include GET metadata.");
        Assert(response.Contains("$3\r\nget\r\n", StringComparison.Ordinal), "COMMAND INFO GET should include get metadata.");
        Assert(response.Contains("$9\r\nmaxmemory\r\n$1\r\n0\r\n", StringComparison.Ordinal), "Default maxmemory should be 0.");
        Assert(response.Contains("$9\r\nmaxmemory\r\n$7\r\n1048576\r\n", StringComparison.Ordinal), "CONFIG SET maxmemory 1mb should update runtime config.");
    }

    [Test]
    public async Task RedisMaxMemoryNoEvictionReturnsOomAsync()
    {
        var processor = new RedisCommandProcessor(
            new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()),
            compatibilityOptions: new RedisCompatibilityOptions
            {
                MaxMemoryBytes = 8,
                MaxMemoryPolicy = "noeviction"
            });

        var response = await ExecuteAsync(processor,
            RespCommand("SET", "a", "1234") +
            RespCommand("SET", "b", "5678"));

        Assert(response.Contains("-OOM command not allowed", StringComparison.Ordinal), "Second SET should fail with stable OOM error under noeviction.");
    }

    [Test]
    public async Task RedisParserMalformedFrameReturnsProtocolErrorAsync()
    {
        var processor = CreateProcessor();
        var malformed = Encoding.UTF8.GetBytes("*1\r\n$4\r\nPING\n");
        await using var input = new MemoryStream(malformed);
        await using var output = new MemoryStream();
        await processor.ProcessAsync(input, output);

        var response = Encoding.UTF8.GetString(output.ToArray());
        Assert(response.Contains("-ERR Protocol error: invalid RESP frame\r\n", StringComparison.Ordinal), "Malformed RESP must return protocol error and not crash.");
    }

    [Test]
    public async Task RedisIncrDecrByOverflowAndRangeAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "counter", "9223372036854775807") +
            RespCommand("INCR", "counter") +
            RespCommand("SET", "counter", "x") +
            RespCommand("DECRBY", "counter", "1"));

        Assert(response.Contains("-ERR value is not an integer or out of range\r\n", StringComparison.Ordinal), "INCR overflow and DECRBY non-integer should return stable range error.");
    }

    [Test]
    public async Task PrefixScanReturnsMatchingKeysAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        await store.PutAsync("user:alice:profile", Encoding.UTF8.GetBytes("1"));
        await store.PutAsync("user:alice:settings", Encoding.UTF8.GetBytes("2"));
        await store.PutAsync("user:bob:profile", Encoding.UTF8.GetBytes("3"));

        var keys = await store.GetKeysWithPrefixAsync("user:alice:");
        AssertSequenceEqual(new[] { "user:alice:profile", "user:alice:settings" }, keys);
    }

    [Test]
    public async Task RangeScanSupportsBoundariesAndReverseOrderAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        await store.PutAsync("k:1", Encoding.UTF8.GetBytes("one"));
        await store.PutAsync("k:2", Encoding.UTF8.GetBytes("two"));
        await store.PutAsync("k:3", Encoding.UTF8.GetBytes("three"));

        var descending = await store.GetKeyRangeAsync("k:1", "k:3", new RangeScanOptions
        {
            IncludeStart = false,
            IncludeEnd = true,
            Descending = true,
            IncludeValues = true
        });

        AssertSequenceEqual(new[] { "k:3", "k:2" }, descending.Select(static item => item.Key));
        AssertEqual("three", Encoding.UTF8.GetString(descending[0].Value!));
        AssertEqual("two", Encoding.UTF8.GetString(descending[1].Value!));
    }

    [Test]
    public async Task RangeScanRejectsInvalidBoundsAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        await store.PutAsync("a", Encoding.UTF8.GetBytes("1"));

        var threw = false;
        try
        {
            _ = await store.GetKeyRangeAsync("z", "a");
        }
        catch (ArgumentException ex) when (ex.Message.Contains("startKey must be lexicographically ≤ endKey.", StringComparison.Ordinal))
        {
            threw = true;
        }

        Assert(threw, "Expected a clear error when startKey is greater than endKey.");
    }

    [Test]
    public async Task PrivacyModeHashesIndexKeysWhileKeepingApiBehaviorAsync()
    {
        const string privacyKeyHex = "00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDDEEFF";
        var innerIndex = new InMemoryKeyIndex();
        var store = new SwarmKeyValueStore(
            new InMemorySwarmClient(),
            innerIndex,
            new SwarmKeyDbOptions
            {
                PrivacyMode = PrivacyMode.ObliviousHashing,
                PrivacyKeyHex = privacyKeyHex
            });

        await store.PutAsync("profile:name", Encoding.UTF8.GetBytes("Ada"));
        var tokenizedKeys = await innerIndex.ListKeysAsync();
        AssertEqual(1, tokenizedKeys.Count);
        Assert(tokenizedKeys[0] != "profile:name", "Underlying index key should be tokenized.");
        Assert(!tokenizedKeys[0].Contains("profile:name", StringComparison.Ordinal), "Tokenized key must not contain plaintext key.");

        AssertEqual("Ada", Encoding.UTF8.GetString((await store.GetAsync("profile:name"))!));
        AssertSequenceEqual(new[] { "profile:name" }, await store.ListKeysAsync());
    }

    [Test]
    public async Task PrivacyModeRequiresLocalManifestForScansAsync()
    {
        const string privacyKeyHex = "11223344556677889900AABBCCDDEEFF11223344556677889900AABBCCDDEEFF";
        var strategy = HmacSha256KeyStrategy.FromHexKey(privacyKeyHex);
        var innerIndex = new InMemoryKeyIndex();
        await innerIndex.SetReferenceAsync(strategy.DeriveToken("hidden:key"), "ref:1");

        var index = new PrivacyPreservingKeyIndex(innerIndex, strategy);
        var threw = false;
        try
        {
            _ = await index.ListKeysAsync();
        }
        catch (PrivacyModeException ex) when (ex.Message.Contains("local key manifest", StringComparison.Ordinal))
        {
            threw = true;
        }

        Assert(threw, "Expected privacy-preserving scans to require a local manifest when tokenized keys already exist.");
    }

    [Test]
    public async Task PrivacyKeyRotationMigratesTokensAsync()
    {
        var oldStrategy = HmacSha256KeyStrategy.FromHexKey("A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1");
        var inner = new InMemoryKeyIndex();
        var index = new PrivacyPreservingKeyIndex(inner, oldStrategy);
        await index.SetReferenceAsync("customer:42", "swarm-ref");
        var oldToken = oldStrategy.DeriveToken("customer:42");
        Assert(await inner.GetReferenceAsync(oldToken) is not null, "Expected old tokenized key to exist before rotation.");

        var rotation = new PrivacyKeyRotationService(index);
        var migrated = await rotation.RotateAsync("B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2");
        AssertEqual(1, migrated);

        var newToken = HmacSha256KeyStrategy.FromHexKey("B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2")
            .DeriveToken("customer:42");
        Assert(await inner.GetReferenceAsync(oldToken) is null, "Expected old token to be removed after rotation.");
        AssertEqual("swarm-ref", await inner.GetReferenceAsync(newToken));
    }

    [Test]
    public async Task ScanAsyncReturnsPaginatedOpaqueCursorAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        await store.PutAsync("page:1", Encoding.UTF8.GetBytes("1"));
        await store.PutAsync("page:2", Encoding.UTF8.GetBytes("2"));
        await store.PutAsync("page:3", Encoding.UTF8.GetBytes("3"));

        var first = await store.ScanAsync(null, 2);
        AssertSequenceEqual(new[] { "page:1", "page:2" }, first.Keys);
        Assert(!string.IsNullOrEmpty(first.NextCursor), "Expected non-empty cursor for additional pages.");
        Assert(!first.NextCursor.Contains("page:2", StringComparison.Ordinal), "Cursor should be opaque, not a raw key.");

        var second = await store.ScanAsync(first.NextCursor, 2);
        AssertSequenceEqual(new[] { "page:3" }, second.Keys);
        AssertEqual(string.Empty, second.NextCursor);
    }

    [Test]
    public async Task QueryAsyncAppliesKeyAndValuePredicatesAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        await store.PutAsync("msg:1", Encoding.UTF8.GetBytes("hello"));
        await store.PutAsync("msg:2", Encoding.UTF8.GetBytes("bye"));
        await store.PutAsync("cfg:1", Encoding.UTF8.GetBytes("hello"));

        var matches = new List<string>();
        await foreach (var item in store.QueryAsync(
                           key => key.StartsWith("msg:", StringComparison.Ordinal),
                           value => Encoding.UTF8.GetString(value).Contains("hello", StringComparison.Ordinal)))
        {
            matches.Add(item.Key);
        }

        AssertSequenceEqual(new[] { "msg:1" }, matches);
    }

    [Test]
    public async Task PersistentFileIndexSupportsRestartQueryingAsync()
    {
        var root = Path.Combine(Path.GetTempPath(), "swarm-keydb-tests", Guid.NewGuid().ToString("N"));
        var indexPath = Path.Combine(root, "index.json");
        Directory.CreateDirectory(root);
        try
        {
            var swarm = new InMemorySwarmClient();
            var store1 = new SwarmKeyValueStore(swarm, new FileKeyIndex(indexPath));
            await store1.PutAsync("orders:001", Encoding.UTF8.GetBytes("first"));
            await store1.PutAsync("orders:002", Encoding.UTF8.GetBytes("second"));
            await store1.PutAsync("profile:001", Encoding.UTF8.GetBytes("other"));

            var store2 = new SwarmKeyValueStore(swarm, new FileKeyIndex(indexPath));
            var prefix = await store2.GetKeysWithPrefixAsync("orders:");
            var range = await store2.GetKeyRangeAsync("orders:001", "orders:999");

            AssertSequenceEqual(new[] { "orders:001", "orders:002" }, prefix);
            AssertSequenceEqual(new[] { "orders:001", "orders:002" }, range.Select(static item => item.Key));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

}
