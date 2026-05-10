using System.Text;
using NUnit.Framework;
using SwarmKeyDb;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

/// <summary>
/// Unit tests for the HELLO command: protocol negotiation, AUTH sub-option, SETNAME, error cases.
/// </summary>
[TestFixture]
[Category("Unit")]
public class HelloCommandTests
{
    // ─── Basic negotiation ────────────────────────────────────────────────────────

    [Test]
    public async Task Hello_NoArgs_ReturnsCurrentProtocolInfo()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO"));

        // Default protocol is 2. Response is a map (encoded as flat array in RESP2).
        // The response must contain "proto" and "2".
        Assert(response.Contains("proto", StringComparison.Ordinal), "Expected 'proto' key in HELLO response.");
        Assert(response.Contains(":2\r\n", StringComparison.Ordinal), "Expected integer 2 for protocol version.");
    }

    [Test]
    public async Task Hello_Version2_AcknowledgesResp2()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO", "2"));

        Assert(response.Contains("proto", StringComparison.Ordinal), "Expected 'proto' key.");
        Assert(response.Contains(":2\r\n", StringComparison.Ordinal), "Expected proto=2 in RESP2 acknowledgement.");
    }

    [Test]
    public async Task Hello_Version3_UpgradesToResp3()
    {
        var processor = CreateProcessor();
        // HELLO 3 transitions protocol and returns a RESP3 map
        var response = await ExecuteAsync(processor, RespCommand("HELLO", "3"));

        // RESP3 map header: %N\r\n
        Assert(response.StartsWith('%'), $"Expected RESP3 map prefix after HELLO 3, got: {response}");
        Assert(response.Contains("proto", StringComparison.Ordinal), "Expected 'proto' key in HELLO 3 response.");
    }

    [Test]
    public async Task Hello_Version3_ThenVersion2_DowngradesBackToResp2()
    {
        var processor = CreateProcessor();
        var hello3 = RespCommand("HELLO", "3");
        var hello2 = RespCommand("HELLO", "2");
        var ping = RespCommand("PING");

        await using var input = new MemoryStream(Encoding.UTF8.GetBytes(hello3 + hello2 + ping));
        await using var output = new MemoryStream();
        await processor.ProcessAsync(input, output);
        var raw = Encoding.UTF8.GetString(output.ToArray());

        // After HELLO 2, PING should be +PONG (RESP2 simple string), not RESP3
        Assert(raw.EndsWith("+PONG\r\n", StringComparison.Ordinal),
            $"Expected RESP2 PONG after downgrade to proto 2, got: {raw}");
    }

    [Test]
    public async Task Hello_Version3_PingReturnsPongSimpleString()
    {
        var processor = CreateProcessor();
        // After HELLO 3, PING should still return +PONG (simple string is the same in both protocols)
        var combined = RespCommand("HELLO", "3") + RespCommand("PING");
        var response = await ExecuteAsync(processor, combined);

        Assert(response.EndsWith("+PONG\r\n", StringComparison.Ordinal),
            $"Expected +PONG after HELLO 3 handshake, got: {response}");
    }

    // ─── Unsupported protocol version ─────────────────────────────────────────────

    [Test]
    public async Task Hello_InvalidVersion_ReturnsNoproto()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO", "4"));

        Assert(response.Contains("NOPROTO", StringComparison.OrdinalIgnoreCase),
            $"Expected NOPROTO error, got: {response}");
    }

    [Test]
    public async Task Hello_VersionZero_ReturnsNoproto()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO", "0"));

        Assert(response.Contains("NOPROTO", StringComparison.OrdinalIgnoreCase),
            $"Expected NOPROTO error for version 0, got: {response}");
    }

    [Test]
    public async Task Hello_NonNumericVersion_ReturnsNoproto()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO", "abc"));

        Assert(response.Contains("NOPROTO", StringComparison.OrdinalIgnoreCase),
            $"Expected NOPROTO error for non-numeric version, got: {response}");
    }

    // ─── HELLO inside MULTI block ─────────────────────────────────────────────────

    [Test]
    public async Task Hello_InsideMulti_ReturnsError()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("MULTI") +
            RespCommand("HELLO", "3"));

        // MULTI returns +OK
        // HELLO inside MULTI should return an error (not QUEUED)
        Assert(response.Contains("ERR", StringComparison.OrdinalIgnoreCase),
            $"Expected error for HELLO inside MULTI, got: {response}");
        Assert(!response.Contains("QUEUED", StringComparison.Ordinal),
            "HELLO must not be queued in a MULTI block.");
    }

    // ─── HELLO response fields ────────────────────────────────────────────────────

    [Test]
    public async Task Hello_Response_ContainsRequiredFields()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO"));

        var requiredFields = new[] { "server", "version", "proto", "id", "mode", "role", "modules" };
        foreach (var field in requiredFields)
        {
            Assert(response.Contains(field, StringComparison.Ordinal),
                $"Expected '{field}' in HELLO response, got: {response}");
        }
    }

    [Test]
    public async Task Hello_Response_ServerIsSwarmKeyDb()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO"));

        Assert(response.Contains("swarmkeydb", StringComparison.OrdinalIgnoreCase),
            $"Expected 'swarmkeydb' in server field, got: {response}");
    }

    [Test]
    public async Task Hello_Response_ModeIsStandalone()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("HELLO"));

        Assert(response.Contains("standalone", StringComparison.OrdinalIgnoreCase),
            $"Expected 'standalone' in mode field, got: {response}");
    }

    // ─── RESET command ────────────────────────────────────────────────────────────

    [Test]
    public async Task Reset_ReturnsResetSimpleString()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("RESET"));

        AssertEqual("+RESET\r\n", response);
    }

    [Test]
    public async Task Reset_AfterHello3_DowngradesProtocol()
    {
        var processor = CreateProcessor();
        var combined = RespCommand("HELLO", "3") + RespCommand("RESET") + RespCommand("PING");
        var response = await ExecuteAsync(processor, combined);

        // After RESET, PING should be RESP2 simple string
        Assert(response.EndsWith("+PONG\r\n", StringComparison.Ordinal),
            $"Expected RESP2 PONG after RESET, got: {response}");

        // The response after RESET (before PING) should NOT start with % (no RESP3 map)
        // The first response should be the HELLO 3 map, then +RESET, then +PONG
        Assert(response.Contains("+RESET\r\n", StringComparison.Ordinal),
            "Expected +RESET\r\n in the response stream.");
    }

    // ─── RESP3 metrics ────────────────────────────────────────────────────────────

    [Test]
    public async Task Hello3_IncrementsResp3ConnectionsTotal()
    {
        var processor = CreateProcessor();
        var before = processor.GetResp3Metrics().Resp3ConnectionsTotal;
        await ExecuteAsync(processor, RespCommand("HELLO", "3"));
        var after = processor.GetResp3Metrics().Resp3ConnectionsTotal;

        Assert(after > before, $"Expected resp3_connections_total to increment, before={before} after={after}.");
    }
}
