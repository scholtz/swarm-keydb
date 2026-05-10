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
public class StringCommandTests
{
    [Test]
    public async Task MGetReturnsNullsForMissingKeysAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "a", "1") +
            RespCommand("MGET", "a", "missing", "b"));

        AssertEqual("+OK\r\n*3\r\n$1\r\n1\r\n$-1\r\n$-1\r\n", response);
    }

    [Test]
    public async Task MSetSetsMultipleKeysAtomicallyAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("MSET", "a", "1", "b", "2", "c", "3") +
            RespCommand("MGET", "a", "b", "c"));

        AssertEqual("+OK\r\n*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n", response);
    }

    [Test]
    public async Task SetExStoresValueWithTtlAsync()
    {
        var processor = CreateProcessor();
        var setExResponse = await ExecuteAsync(processor, RespCommand("SETEX", "session:token", "300", "abc123"));
        AssertEqual("+OK\r\n", setExResponse);

        var ttlResponse = await ExecuteAsync(processor, RespCommand("TTL", "session:token"));
        var ttl = ParseIntegerResponse(ttlResponse);
        Assert(ttl is > 0 and <= 300, "TTL should be within expected range.");
    }

    [Test]
    public async Task MSetNxDoesNotPartiallyWriteWhenBlockedAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "a", "existing") +
            RespCommand("MSETNX", "a", "new", "b", "new-b") +
            RespCommand("MGET", "a", "b"));

        AssertEqual("+OK\r\n:0\r\n*2\r\n$8\r\nexisting\r\n$-1\r\n", response);
    }

    [Test]
    public async Task RedisManagementCommandsBackupRestoreAndRotateAsync()
    {
        var swarm = new MutableSwarmClient();
        var keyProvider = new MutableEncryptionKeyProvider(new EncryptionOptions
        {
            Enabled = true,
            EthPrivateKeyHex = MakeKeyHex()
        });
        var store = new EncryptingKeyValueStore(
            new SwarmKeyValueStore(swarm, new InMemoryKeyIndex()),
            keyProvider,
            NullLogger<EncryptingKeyValueStore>.Instance);
        var backupService = new BackupService(store, swarm, keyProvider);
        var restoreService = new RestoreService(backupService, store, keyProvider);
        var rotationService = new KeyRotationService(store, swarm, keyProvider, backupService);
        var processor = new RedisCommandProcessor(store, backupService: backupService, restoreService: restoreService, keyRotationService: rotationService);
        var oldKey = keyProvider.GetOptions().EthPrivateKeyHex!;

        AssertEqual("+OK\r\n", await ExecuteAsync(processor, RespCommand("SET", "managed:key", "managed-value")));

        var backupResponse = await ExecuteAsync(processor, RespCommand("BACKUP"));
        Assert(backupResponse.StartsWith("$", StringComparison.Ordinal) && backupResponse.Contains("swarm://", StringComparison.Ordinal),
            "BACKUP should return a bulk-string swarm:// reference.");

        var newKey = MakeKeyHex();
        var rotateResponse = await ExecuteAsync(processor, RespCommand("ROTATEKEY", oldKey, newKey));
        Assert(rotateResponse.Contains("swarm://", StringComparison.Ordinal), "ROTATEKEY should return a manifest reference.");
        AssertEqual("$13\r\nmanaged-value\r\n", await ExecuteAsync(processor, RespCommand("GET", "managed:key")));

        var restoredIndex = new InMemoryKeyIndex();
        var restoredProvider = new MutableEncryptionKeyProvider(new EncryptionOptions());
        var restoredStore = new EncryptingKeyValueStore(
            new SwarmKeyValueStore(swarm, restoredIndex),
            restoredProvider,
            NullLogger<EncryptingKeyValueStore>.Instance);
        var restoredBackupService = new BackupService(restoredStore, swarm, restoredProvider);
        var restoredRestoreService = new RestoreService(restoredBackupService, restoredStore, restoredProvider);
        var restoredProcessor = new RedisCommandProcessor(restoredStore, backupService: restoredBackupService, restoreService: restoredRestoreService);

        var backupReference = backupResponse.Split("\r\n", StringSplitOptions.RemoveEmptyEntries)[1];
        AssertEqual(":1\r\n", await ExecuteAsync(restoredProcessor, RespCommand("RESTOREDB", backupReference, oldKey)));
        AssertEqual("$13\r\nmanaged-value\r\n", await ExecuteAsync(restoredProcessor, RespCommand("GET", "managed:key")));
    }

    [Test]
    public async Task RedisSwarmResyncCommandSupportsModesAndErrorsAsync()
    {
        var coordinator = new RecordingResyncCoordinator();
        var processor = new RedisCommandProcessor(
            new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()),
            resyncCoordinator: coordinator);

        var auto = await ExecuteAsync(processor, RespCommand("SWARM.RESYNC"));
        Assert(auto.Contains("\"status\":\"ok\"", StringComparison.Ordinal), "Expected SWARM.RESYNC auto success payload.");

        var partial = await ExecuteAsync(processor, RespCommand("SWARM.RESYNC", "PARTIAL"));
        Assert(partial.Contains("\"mode\":\"partial\"", StringComparison.Ordinal), "Expected explicit partial mode in response.");

        var invalidMode = await ExecuteAsync(processor, RespCommand("SWARM.RESYNC", "invalid"));
        AssertEqual("-ERR invalid resync mode. expected PARTIAL or FULL\r\n", invalidMode);

        var wrongArity = await ExecuteAsync(processor, RespCommand("SWARM.RESYNC", "FULL", "EXTRA"));
        AssertEqual("-ERR wrong number of arguments for 'SWARM.RESYNC' command\r\n", wrongArity);

        var unavailable = await ExecuteAsync(CreateProcessor(), RespCommand("SWARM.RESYNC"));
        AssertEqual("-ERR SWARM.RESYNC is not available.\r\n", unavailable);
    }

    [Test]
    public async Task RedisCommandLoggingIncludesCorrelationIdsAsync()
    {
        var logger = new CaptureLogger<RedisCommandProcessor>();
        var processor = new RedisCommandProcessor(
            new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()),
            logger: logger);

        _ = await ExecuteAsync(processor, RespCommand("PING"));

        Assert(logger.Scopes.Count > 0, "Expected at least one logging scope.");
        Assert(logger.Scopes.Any(scope => scope.TryGetValue("correlationId", out var value) && !string.IsNullOrWhiteSpace(value)),
            "Expected correlationId in command logging scope.");
    }

    [Test]
    public async Task RedisSetReturnsErrorWhenBackendUploadFailsAsync()
    {
        var failingStore = new SwarmKeyValueStore(
            new FailingSwarmClient(new HttpRequestException("Not Found", null, HttpStatusCode.NotFound)),
            new InMemoryKeyIndex());
        var processor = new RedisCommandProcessor(failingStore);

        var response = await ExecuteAsync(processor, RespCommand("SET", "bee:key", "value"));
        AssertEqual("-ERR backend storage unavailable\r\n", response);
    }

    [Test]
    public async Task RedisGetReturnsErrorWhenBackendDownloadFailsAsync()
    {
        var index = new InMemoryKeyIndex();
        await index.SetReferenceAsync("bee:key", "swarm-ref");

        var failingStore = new SwarmKeyValueStore(
            new FailingSwarmClient(
                new HttpRequestException("Not Found", null, HttpStatusCode.NotFound),
                new HttpRequestException("Not Found", null, HttpStatusCode.NotFound)),
            index);
        var processor = new RedisCommandProcessor(failingStore);

        var response = await ExecuteAsync(processor, RespCommand("GET", "bee:key"));
        AssertEqual("-ERR backend storage unavailable\r\n", response);
    }

}
