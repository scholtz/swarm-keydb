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
public class CliTests
{
    [Test]
    public async Task CliSupportsDataCommandsAsync()
    {
        var swarm = new InMemorySwarmClient();
        var index = new InMemoryKeyIndex();
        var options = new CliExecutionOptions
        {
            SwarmClientFactory = _ => swarm,
            KeyIndexFactory = _ => index,
            EnvironmentFactory = static () => new EnvironmentSnapshot
            {
                Home = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-tests", Guid.NewGuid().ToString("N")),
                BeeUrl = "http://localhost:1633/",
                BatchId = "batch-id"
            }
        };

        var putResult = await RunCliAsync(new[] { "put", "user:alice", "{\"name\":\"Alice\"}" }, options);
        AssertEqual(0, putResult.ExitCode);
        AssertEqual("OK user:alice", putResult.Stdout.Trim());

        var getResult = await RunCliAsync(new[] { "get", "user:alice", "--output", "json" }, options);
        AssertEqual(0, getResult.ExitCode);
        var getPayload = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(getResult.Stdout.Trim());
        Assert(getPayload is not null, "Expected get JSON payload.");
        Assert(getPayload!["found"].GetBoolean(), "Expected key to exist.");
        AssertEqual("{\"name\":\"Alice\"}", getPayload["value"].GetString());

        await RunCliAsync(new[] { "put", "user:bob", "2" }, options);
        await RunCliAsync(new[] { "put", "profile:charlie", "3" }, options);

        var listResult = await RunCliAsync(new[] { "list", "--prefix", "user:" }, options);
        AssertEqual(0, listResult.ExitCode);
        Assert(listResult.Stdout.Contains("user:alice", StringComparison.Ordinal), "List should include prefixed key.");
        Assert(listResult.Stdout.Contains("user:bob", StringComparison.Ordinal), "List should include second prefixed key.");
        Assert(!listResult.Stdout.Contains("profile:charlie", StringComparison.Ordinal), "List should filter non-prefixed key.");

        var scanResult = await RunCliAsync(new[] { "scan", "--from", "user:a", "--to", "user:z" }, options);
        AssertEqual(0, scanResult.ExitCode);
        Assert(scanResult.Stdout.Contains("user:alice", StringComparison.Ordinal), "Scan should include user:alice.");
        Assert(scanResult.Stdout.Contains("user:bob", StringComparison.Ordinal), "Scan should include user:bob.");

        var statsResult = await RunCliAsync(new[] { "stats", "--output", "json" }, options);
        AssertEqual(0, statsResult.ExitCode);
        var statsPayload = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(statsResult.Stdout.Trim());
        Assert(statsPayload is not null, "Expected stats JSON payload.");
        Assert(statsPayload!["keyCount"].GetInt32() >= 3, "Stats should report all inserted keys.");
        Assert(statsPayload["storageBytes"].GetInt64() > 0, "Stats should include storage usage.");

        var deleteResult = await RunCliAsync(new[] { "delete", "user:alice" }, options);
        AssertEqual(0, deleteResult.ExitCode);
        AssertEqual("1", deleteResult.Stdout.Trim());

        var missingResult = await RunCliAsync(new[] { "get", "user:alice" }, options);
        AssertEqual(1, missingResult.ExitCode);
        Assert(missingResult.Stderr.Contains("Key not found: user:alice", StringComparison.Ordinal), "Expected explicit missing key error message.");
    }

    [Test]
    public async Task CliBackupRestoreAndRotateKeyAsync()
    {
        var swarm = new MutableSwarmClient();
        var sourceHome = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-rotate-source", Guid.NewGuid().ToString("N"));
        var targetHome = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-rotate-target", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(sourceHome);
        Directory.CreateDirectory(targetHome);

        var oldKeyPath = Path.Combine(sourceHome, "old.key");
        var newKeyPath = Path.Combine(sourceHome, "new.key");
        await File.WriteAllTextAsync(oldKeyPath, MakeKeyHex());
        await File.WriteAllTextAsync(newKeyPath, MakeKeyHex());

        CliExecutionOptions CreateOptions(string home) => new()
        {
            SwarmClientFactory = _ => swarm,
            KeyIndexFactory = path => new FileKeyIndex(path),
            EnvironmentFactory = () => new EnvironmentSnapshot
            {
                Home = home,
                BeeUrl = "http://localhost:1633/",
                BatchId = "batch-id"
            }
        };

        var sourceOptions = CreateOptions(sourceHome);
        var putResult = await RunCliAsync(new[] { "put", "secure:key", "secret-value" }, sourceOptions);
        AssertEqual(0, putResult.ExitCode);

        var rotateResult = await RunCliAsync(new[] { "rotate-key", "--old-key", oldKeyPath, "--new-key", newKeyPath }, sourceOptions);
        AssertEqual(0, rotateResult.ExitCode);
        Assert(rotateResult.Stdout.Trim().StartsWith("swarm://", StringComparison.Ordinal), "Rotate command should return a rotation manifest reference.");
        Assert(rotateResult.Stderr.Contains("Rotating key:", StringComparison.Ordinal), "Rotate command should emit progress.");

        var encryptedRead = await RunCliAsync(new[] { "--key", newKeyPath, "get", "secure:key" }, sourceOptions);
        AssertEqual(0, encryptedRead.ExitCode);
        AssertEqual("secret-value", encryptedRead.Stdout.Trim());

        var backupPath = Path.Combine(sourceHome, "backup.ref");
        var backupResult = await RunCliAsync(new[] { "--key", newKeyPath, "backup", "--out", backupPath }, sourceOptions);
        AssertEqual(0, backupResult.ExitCode);
        Assert(File.Exists(backupPath), "Backup command should write the swarm reference when --out is provided.");
        var backupReference = (await File.ReadAllTextAsync(backupPath)).Trim();
        Assert(backupReference.StartsWith("swarm://", StringComparison.Ordinal), "Backup file should contain a swarm:// reference.");

        var targetOptions = CreateOptions(targetHome);
        var restoreResult = await RunCliAsync(new[] { "restore", "--ref", backupReference, "--key", newKeyPath }, targetOptions);
        AssertEqual(0, restoreResult.ExitCode);

        var restoredRead = await RunCliAsync(new[] { "--key", newKeyPath, "get", "secure:key" }, targetOptions);
        AssertEqual(0, restoredRead.ExitCode);
        AssertEqual("secret-value", restoredRead.Stdout.Trim());
    }

    [Test]
    public async Task CliConfigSetAndGetPersistsSettingsAsync()
    {
        var home = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-tests", Guid.NewGuid().ToString("N"));
        var options = new CliExecutionOptions
        {
            EnvironmentFactory = () => new EnvironmentSnapshot
            {
                Home = home
            }
        };

        var setResult = await RunCliAsync(new[] { "config", "set", "--bee-url", "http://localhost:1733/", "--batch-id", "batch-123", "--output", "table" }, options);
        AssertEqual(0, setResult.ExitCode);
        var configPath = Path.Combine(home, ".swarmkeydb", "config.json");
        Assert(File.Exists(configPath), "Config file should be created.");

        var getResult = await RunCliAsync(new[] { "config", "get", "--output", "json" }, options);
        AssertEqual(0, getResult.ExitCode);
        var payload = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(getResult.Stdout.Trim());
        Assert(payload is not null, "Expected config JSON payload.");
        AssertEqual("http://localhost:1733/", payload!["BeeUrl"].GetString());
        AssertEqual("batch-123", payload["BatchId"].GetString());
    }

    [Test]
    public async Task CliPutValidatesValueSourceArgumentsAsync()
    {
        var options = CreateCliTestOptions();

        var noValueResult = await RunCliAsync(new[] { "put", "k" }, options);
        AssertEqual(1, noValueResult.ExitCode);
        Assert(noValueResult.Stderr.Contains("put requires <value> or --file <path>.", StringComparison.Ordinal), "Expected validation message for missing value.");

        var bothSourcesResult = await RunCliAsync(new[] { "put", "k", "v", "--file", "x" }, options);
        AssertEqual(1, bothSourcesResult.ExitCode);
        Assert(bothSourcesResult.Stderr.Contains("Use either inline <value> or --file, not both.", StringComparison.Ordinal), "Expected validation message for conflicting value sources.");
    }

    [Test]
    public async Task CliUsesEnvironmentVariableOverridesAsync()
    {
        var options = new CliExecutionOptions
        {
            EnvironmentFactory = static () => new EnvironmentSnapshot
            {
                Home = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-env"),
                BeeUrl = "http://127.0.0.1:1/",
                BatchId = "env-batch"
            }
        };

        var result = await RunCliAsync(new[] { "put", "env:test", "1" }, options);
        AssertEqual(1, result.ExitCode);
        Assert(result.Stderr.Contains("Bee node unreachable", StringComparison.Ordinal), "Expected Bee connectivity error.");
    }

    [Test]
    public async Task CliDeleteNamespaceRemovesPrefixedKeysAsync()
    {
        var options = CreateCliTestOptions();

        await RunCliAsync(new[] { "put", "users:alice:profile", "Alice" }, options);
        await RunCliAsync(new[] { "put", "users:alice:settings", "dark" }, options);
        await RunCliAsync(new[] { "put", "users:bob:profile", "Bob" }, options);

        var deleteResult = await RunCliAsync(new[] { "delete-namespace", "users:alice:" }, options);
        AssertEqual(0, deleteResult.ExitCode);
        AssertEqual("2", deleteResult.Stdout.Trim());

        var listResult = await RunCliAsync(new[] { "list" }, options);
        Assert(!listResult.Stdout.Contains("users:alice:", StringComparison.Ordinal), "Alice's keys should be deleted.");
        Assert(listResult.Stdout.Contains("users:bob:profile", StringComparison.Ordinal), "Bob's key should remain.");
    }

}
