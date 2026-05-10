using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using NUnit.Framework;
using SwarmKeyDb;
using SwarmKeyDb.Server;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

[TestFixture]
[Category("Integration")]
public class BeeBackendIntegrationTests
{
    private const string TestKey = "test:swarm-check";
    private const string TestValue = "hello-bee";

    [Test]
    public async Task SetAndGetKey_StoresAndRetrievesFromBeeAsync()
    {
        var beeUrl = Environment.GetEnvironmentVariable("SWARM_KEYDB_TEST_BEE_URL")
            ?? Environment.GetEnvironmentVariable("BEE_URL") ?? "https://bzz.limo";
        var batchId = Environment.GetEnvironmentVariable("SWARM_KEYDB_TEST_BEE_POSTAGE_BATCH_ID")
            ?? Environment.GetEnvironmentVariable("BEE_POSTAGE_BATCH_ID") ?? "BATCH_ID";

        if (string.IsNullOrWhiteSpace(beeUrl) || string.IsNullOrWhiteSpace(batchId))
        {
            NUnit.Framework.Assert.Ignore(
                "Real Bee integration requires SWARM_KEYDB_TEST_BEE_URL (or BEE_URL) " +
                "and SWARM_KEYDB_TEST_BEE_POSTAGE_BATCH_ID (or BEE_POSTAGE_BATCH_ID).");
        }

        var configuredUri = new Uri(beeUrl!, UriKind.Absolute);
        if (batchId == "BATCH_ID")
        {
            NUnit.Framework.Assert.Ignore(
                "Bee integration test requires a writable Bee API endpoint and real postage batch id.");
        }

        var beeUri = configuredUri;
        var payload = Encoding.UTF8.GetBytes(TestValue + ":" + Guid.NewGuid().ToString("N"));
        var key = TestKey + ":" + Guid.NewGuid().ToString("N");

        using var httpClient = new HttpClient { BaseAddress = beeUri };
        BeeSwarmClient? beeClient = null;
        try
        {
            beeClient = new BeeSwarmClient(httpClient, batchId!);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("read gateway", StringComparison.OrdinalIgnoreCase))
        {
            NUnit.Framework.Assert.Ignore(
                "Bee integration test requires a writable Bee API endpoint and real postage batch id.");
        }

        NUnit.Framework.Assert.That(beeClient, Is.Not.Null);
        var index = new InMemoryKeyIndex();
        var store = new SwarmKeyValueStore(beeClient!, index);
        var processor = new RedisCommandProcessor(store);

        var setResp = await ExecuteAsync(processor, RespCommand("SET", key, Encoding.UTF8.GetString(payload)));
        AssertEqual("+OK\r\n", setResp);

        var backendMeta = await ExecuteAsync(processor, RespCommand("BACKENDMETA", key));
        var swarmReference = ParseSwarmReferenceFromBackendMeta(backendMeta);
        NUnit.Framework.Assert.That(string.IsNullOrWhiteSpace(swarmReference), Is.False, "BACKENDMETA should include a swarm reference.");

        var resp = await httpClient.GetAsync($"/bytes/{swarmReference}");
        NUnit.Framework.Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.OK), "Bee node should return the object");
        var bytes = await resp.Content.ReadAsByteArrayAsync();
        NUnit.Framework.Assert.That(bytes.Length > 0, Is.True, "Downloaded object should not be empty.");
    }

    [Test]
    public void ThrowsOnMissingBatchId()
    {
        NUnit.Framework.Assert.Throws<ArgumentException>(() => new BeeSwarmClient(new Uri("http://localhost:1633"), ""));
    }

    private static string ParseSwarmReferenceFromBackendMeta(string backendMetaResponse)
    {
        var lines = backendMetaResponse
            .Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length < 2 || !lines[0].StartsWith('$'))
        {
            return string.Empty;
        }

        var metadataJson = lines[1];
        using var doc = JsonDocument.Parse(metadataJson);
        return doc.RootElement.TryGetProperty("swarmReference", out var referenceProperty)
            ? referenceProperty.GetString() ?? string.Empty
            : string.Empty;
    }
}
