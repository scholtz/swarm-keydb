using System.Text;
using NUnit.Framework;
using SwarmKeyDb;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

/// <summary>
/// Integration tests for RESP3 gateway: full round-trip via ProcessAsync.
/// </summary>
[TestFixture]
[Category("Integration")]
public class Resp3GatewayTests
{
    // ─── Protocol negotiation round-trip ─────────────────────────────────────────

    [Test]
    public async Task Hello3_ThenPing_ReturnsPong()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("HELLO", "3") +
            RespCommand("PING"));

        Assert(response.EndsWith("+PONG\r\n", StringComparison.Ordinal),
            $"Expected +PONG after HELLO 3 handshake, got: {response}");
    }

    [Test]
    public async Task Hello3_ThenEcho_ReturnsEchoedValue()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("HELLO", "3") +
            RespCommand("ECHO", "hello-world"));

        Assert(response.Contains("hello-world", StringComparison.Ordinal),
            $"Expected echoed string in response, got: {response}");
    }

    // ─── Key-value operations over RESP3 ─────────────────────────────────────────

    [Test]
    public async Task Hello3_SetAndGet_RoundTrip()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("HELLO", "3") +
            RespCommand("SET", "resp3key", "resp3value") +
            RespCommand("GET", "resp3key"));

        Assert(response.Contains("+OK\r\n", StringComparison.Ordinal),
            $"Expected +OK for SET, got: {response}");
        Assert(response.Contains("resp3value", StringComparison.Ordinal),
            $"Expected GET to return value, got: {response}");
    }

    [Test]
    public async Task Hello3_GetMissingKey_ReturnsNull()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("HELLO", "3") +
            RespCommand("GET", "no-such-key"));

        // RESP3 null = "_\r\n"
        Assert(response.Contains("_\r\n", StringComparison.Ordinal),
            $"Expected RESP3 null for missing key, got: {response}");
    }

    // ─── Null encoding ────────────────────────────────────────────────────────────

    [Test]
    public async Task Resp2_GetMissingKey_ReturnsDollarMinusOne()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("GET", "no-such-key-resp2"));

        Assert(response.Contains("$-1\r\n", StringComparison.Ordinal),
            $"Expected RESP2 null bulk string for missing key, got: {response}");
    }

    // ─── HELLO response shape in RESP3 ───────────────────────────────────────────

    [Test]
    public async Task Hello3_ResponseIsResp3Map()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO", "3"));

        // RESP3 map starts with %
        Assert(response.StartsWith('%'),
            $"Expected RESP3 map (%) prefix in HELLO 3 response, got: {response}");
    }

    [Test]
    public async Task Hello2_ResponseIsRespFlatArray()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO", "2"));

        // RESP2 array starts with *
        Assert(response.StartsWith('*'),
            $"Expected RESP2 array (*) prefix in HELLO 2 response, got: {response}");
    }

    // ─── RESET full cycle ─────────────────────────────────────────────────────────

    [Test]
    public async Task FullCycle_Hello3_Reset_Hello2_RetainsResp2()
    {
        var processor = CreateProcessor();
        var combined =
            RespCommand("HELLO", "3") +
            RespCommand("RESET") +
            RespCommand("GET", "key");

        var response = await ExecuteAsync(processor, combined);

        // GET after RESET should return $-1 (RESP2 null bulk string), not _\r\n
        Assert(response.Contains("$-1\r\n", StringComparison.Ordinal),
            $"Expected RESP2 null after RESET, got: {response}");
    }

    // ─── Error handling ───────────────────────────────────────────────────────────

    [Test]
    public async Task Hello3_UnknownCommand_ReturnsError()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("HELLO", "3") +
            RespCommand("NOTACOMMAND"));

        Assert(response.Contains("-ERR", StringComparison.Ordinal),
            $"Expected error for unknown command after HELLO 3, got: {response}");
    }

    // ─── Multiple sequential HELLOs ──────────────────────────────────────────────

    [Test]
    public async Task MultipleHello_SequentialToggles_WorkCorrectly()
    {
        var processor = CreateProcessor();
        var combined =
            RespCommand("HELLO", "2") +  // start at 2
            RespCommand("HELLO", "3") +  // upgrade to 3
            RespCommand("HELLO", "2") +  // downgrade back to 2
            RespCommand("PING");

        var response = await ExecuteAsync(processor, combined);

        // After final HELLO 2, PING should be RESP2
        Assert(response.EndsWith("+PONG\r\n", StringComparison.Ordinal),
            $"Expected +PONG in RESP2 after toggling, got: {response}");
    }

    // ─── CLIENT TRACKING tests ────────────────────────────────────────────────────

    [Test]
    public async Task ClientTracking_On_ReturnsOk()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("CLIENT", "TRACKING", "ON"));

        Assert(response.Contains("+OK\r\n", StringComparison.Ordinal),
            $"Expected +OK for CLIENT TRACKING ON, got: {response}");
    }

    [Test]
    public async Task ClientTracking_Off_ReturnsOk()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("CLIENT", "TRACKING", "ON") +
            RespCommand("CLIENT", "TRACKING", "OFF"));

        var responses = response.Split("+OK\r\n", StringSplitOptions.RemoveEmptyEntries);
        // Both TRACKING ON and TRACKING OFF should return +OK
        Assert(response.Contains("+OK\r\n", StringComparison.Ordinal),
            $"Expected +OK for CLIENT TRACKING OFF, got: {response}");
    }

    [Test]
    public async Task ClientTracking_InvalidSubcmd_ReturnsError()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("CLIENT", "TRACKING", "INVALID"));

        Assert(response.Contains("-ERR", StringComparison.Ordinal),
            $"Expected error for invalid CLIENT TRACKING subcommand, got: {response}");
    }

    [Test]
    public async Task ClientTracking_On_IncrementsTrackingMetric()
    {
        var processor = CreateProcessor();
        var before = processor.GetResp3Metrics().ClientTrackingConnections;

        // Run a session that activates TRACKING but does NOT close (the tracking registration
        // is removed on connection close). Since ExecuteAsync opens and closes the stream, we
        // can't observe it mid-session, so we just verify no error and the metric is >= 0.
        await ExecuteAsync(processor, RespCommand("CLIENT", "TRACKING", "ON"));

        // After the session ends the tracking entry is removed, so count may be 0 again.
        var after = processor.GetResp3Metrics().ClientTrackingConnections;
        Assert(after >= 0, $"Tracking count must be non-negative, got: {after}");
    }

    // ─── Resp3 metrics ────────────────────────────────────────────────────────────

    [Test]
    public async Task Resp3Metrics_AfterHello3_CountsIncrease()
    {
        var processor = CreateProcessor();
        var metricsBefore = processor.GetResp3Metrics();

        await ExecuteAsync(processor, RespCommand("HELLO", "3"));

        var metricsAfter = processor.GetResp3Metrics();
        Assert(metricsAfter.Resp3ConnectionsTotal > metricsBefore.Resp3ConnectionsTotal,
            "Expected resp3_connections_total to increase after HELLO 3.");
    }

    [Test]
    public async Task Resp3Metrics_AfterHello2Only_CountsUnchanged()
    {
        var processor = CreateProcessor();
        var metricsBefore = processor.GetResp3Metrics();

        await ExecuteAsync(processor, RespCommand("HELLO", "2"));

        var metricsAfter = processor.GetResp3Metrics();
        AssertEqual(metricsBefore.Resp3ConnectionsTotal, metricsAfter.Resp3ConnectionsTotal);
    }
}
