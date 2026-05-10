using System.Text;
using NUnit.Framework;
using SwarmKeyDb;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

[TestFixture]
[Category("Unit")]
public class Resp3EncoderTests
{
    // Helper: write a RespValue at the given protocol version and return raw bytes as a UTF-8 string
    private static async Task<string> EncodeAsync(RespValue value, int protocolVersion)
    {
        await using var ms = new MemoryStream();
        var writer = new RespWriter(ms) { ProtocolVersion = protocolVersion };
        await writer.WriteAsync(value);
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    // ─── Simple String ──────────────────────────────────────────────────────────

    [Test]
    public async Task SimpleString_Resp2_EmitsPlus()
    {
        var result = await EncodeAsync(RespValue.SimpleString("OK"), 2);
        AssertEqual("+OK\r\n", result);
    }

    [Test]
    public async Task SimpleString_Resp3_EmitsPlus()
    {
        var result = await EncodeAsync(RespValue.SimpleString("OK"), 3);
        AssertEqual("+OK\r\n", result);
    }

    // ─── Error ───────────────────────────────────────────────────────────────────

    [Test]
    public async Task Error_Resp2_EmitsMinus()
    {
        var result = await EncodeAsync(RespValue.Error("ERR something"), 2);
        AssertEqual("-ERR something\r\n", result);
    }

    // ─── Integer ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Integer_BothVersions_EmitsColon()
    {
        var result2 = await EncodeAsync(RespValue.IntegerValue(42), 2);
        var result3 = await EncodeAsync(RespValue.IntegerValue(42), 3);
        AssertEqual(":42\r\n", result2);
        AssertEqual(":42\r\n", result3);
    }

    // ─── BulkString ──────────────────────────────────────────────────────────────

    [Test]
    public async Task BulkString_Resp2_NullEmitsDollarMinusOne()
    {
        var result = await EncodeAsync(RespValue.BulkString((byte[]?)null), 2);
        AssertEqual("$-1\r\n", result);
    }

    [Test]
    public async Task BulkString_Resp3_NullEmitsUnderscore()
    {
        var result = await EncodeAsync(RespValue.BulkString((byte[]?)null), 3);
        AssertEqual("_\r\n", result);
    }

    [Test]
    public async Task BulkString_Resp2_ValueEmitsDollar()
    {
        var result = await EncodeAsync(RespValue.BulkString("hello"), 2);
        AssertEqual("$5\r\nhello\r\n", result);
    }

    // ─── Array ───────────────────────────────────────────────────────────────────

    [Test]
    public async Task Array_Null_Resp2_EmitsStarMinusOne()
    {
        var result = await EncodeAsync(RespValue.NullArray(), 2);
        AssertEqual("*-1\r\n", result);
    }

    [Test]
    public async Task Array_Resp2_EmitsStar()
    {
        var result = await EncodeAsync(
            RespValue.Array(new[] { RespValue.BulkString("a"), RespValue.BulkString("b") }), 2);
        AssertEqual("*2\r\n$1\r\na\r\n$1\r\nb\r\n", result);
    }

    // ─── RESP3: Map ──────────────────────────────────────────────────────────────

    [Test]
    public async Task Map_Resp3_EmitsPercent()
    {
        var map = RespValue.Map(new[]
        {
            RespValue.BulkString("key"), RespValue.BulkString("val")
        });
        var result = await EncodeAsync(map, 3);
        AssertEqual("%1\r\n$3\r\nkey\r\n$3\r\nval\r\n", result);
    }

    [Test]
    public async Task Map_Resp2_DegradesFlatArray()
    {
        var map = RespValue.Map(new[]
        {
            RespValue.BulkString("key"), RespValue.BulkString("val")
        });
        var result = await EncodeAsync(map, 2);
        AssertEqual("*2\r\n$3\r\nkey\r\n$3\r\nval\r\n", result);
    }

    // ─── RESP3: Set ───────────────────────────────────────────────────────────────

    [Test]
    public async Task Set_Resp3_EmitsTilde()
    {
        var set = RespValue.Set(new[] { RespValue.BulkString("a"), RespValue.BulkString("b") });
        var result = await EncodeAsync(set, 3);
        AssertEqual("~2\r\n$1\r\na\r\n$1\r\nb\r\n", result);
    }

    [Test]
    public async Task Set_Resp2_DegradesStar()
    {
        var set = RespValue.Set(new[] { RespValue.BulkString("a"), RespValue.BulkString("b") });
        var result = await EncodeAsync(set, 2);
        AssertEqual("*2\r\n$1\r\na\r\n$1\r\nb\r\n", result);
    }

    // ─── RESP3: Double ───────────────────────────────────────────────────────────

    [Test]
    public async Task Double_Resp3_EmitsComma()
    {
        var d = RespValue.Double(3.14);
        var result = await EncodeAsync(d, 3);
        Assert(result.StartsWith(','), "Expected comma prefix for double in RESP3.");
        Assert(result.EndsWith("\r\n", StringComparison.Ordinal), "Expected CRLF suffix.");
        Assert(result.Contains("3.14", StringComparison.Ordinal), "Expected the double value in response.");
    }

    [Test]
    public async Task Double_Resp2_DegradesBulkString()
    {
        var d = RespValue.Double(3.14);
        var result = await EncodeAsync(d, 2);
        Assert(result.StartsWith('$'), "Expected bulk-string prefix for double in RESP2.");
    }

    // ─── RESP3: Boolean ──────────────────────────────────────────────────────────

    [Test]
    public async Task Boolean_True_Resp3_EmitsHashT()
    {
        var result = await EncodeAsync(RespValue.Boolean(true), 3);
        AssertEqual("#t\r\n", result);
    }

    [Test]
    public async Task Boolean_False_Resp3_EmitsHashF()
    {
        var result = await EncodeAsync(RespValue.Boolean(false), 3);
        AssertEqual("#f\r\n", result);
    }

    [Test]
    public async Task Boolean_True_Resp2_DegradesToOne()
    {
        var result = await EncodeAsync(RespValue.Boolean(true), 2);
        AssertEqual(":1\r\n", result);
    }

    [Test]
    public async Task Boolean_False_Resp2_DegradesToZero()
    {
        var result = await EncodeAsync(RespValue.Boolean(false), 2);
        AssertEqual(":0\r\n", result);
    }

    // ─── RESP3: BigNumber ─────────────────────────────────────────────────────────

    [Test]
    public async Task BigNumber_Resp3_EmitsOpenParen()
    {
        var result = await EncodeAsync(RespValue.BigNumber("123456789012345678901234567890"), 3);
        AssertEqual("(123456789012345678901234567890\r\n", result);
    }

    [Test]
    public async Task BigNumber_Resp2_DegradesBulkString()
    {
        var result = await EncodeAsync(RespValue.BigNumber("12345"), 2);
        AssertEqual("$5\r\n12345\r\n", result);
    }

    // ─── RESP3: VerbatimString ────────────────────────────────────────────────────

    [Test]
    public async Task VerbatimString_Resp3_EmitsEquals()
    {
        var result = await EncodeAsync(RespValue.VerbatimString("txt", "Hello"), 3);
        // Format: =<len>\r\n<3charenc>:<data>\r\n where raw = "txt:Hello" = 9 chars
        AssertEqual("=9\r\ntxt:Hello\r\n", result);
    }

    [Test]
    public async Task VerbatimString_Resp2_DegradesBulkString_DataOnly()
    {
        var result = await EncodeAsync(RespValue.VerbatimString("txt", "Hello"), 2);
        // Should strip encoding prefix and return just the data portion
        AssertEqual("$5\r\nHello\r\n", result);
    }

    // ─── RESP3: Null ──────────────────────────────────────────────────────────────

    [Test]
    public async Task Null_Resp3_EmitsUnderscore()
    {
        var result = await EncodeAsync(RespValue.Null(), 3);
        AssertEqual("_\r\n", result);
    }

    [Test]
    public async Task Null_Resp2_EmitsDollarMinusOne()
    {
        var result = await EncodeAsync(RespValue.Null(), 2);
        AssertEqual("$-1\r\n", result);
    }

    // ─── RESP3: BlobError ─────────────────────────────────────────────────────────

    [Test]
    public async Task BlobError_Resp3_EmitsBang()
    {
        const string message = "ERR some error";
        var result = await EncodeAsync(RespValue.BlobError(message), 3);
        // Format: !<len>\r\n<msg>\r\n
        var expected = $"!{Encoding.UTF8.GetByteCount(message)}\r\n{message}\r\n";
        AssertEqual(expected, result);
    }

    [Test]
    public async Task BlobError_Resp2_DegradesToMinusError()
    {
        var result = await EncodeAsync(RespValue.BlobError("ERR some error"), 2);
        AssertEqual("-ERR some error\r\n", result);
    }

    // ─── RESP3: Push ──────────────────────────────────────────────────────────────

    [Test]
    public async Task Push_Resp3_EmitsGreater()
    {
        var push = RespValue.Push(new[]
        {
            RespValue.BulkString("message"),
            RespValue.BulkString("channel"),
            RespValue.BulkString("payload")
        });
        var result = await EncodeAsync(push, 3);
        Assert(result.StartsWith(">3\r\n", StringComparison.Ordinal), $"Expected >3 prefix, got: {result}");
    }

    [Test]
    public async Task Push_Resp2_DegradesStar()
    {
        var push = RespValue.Push(new[]
        {
            RespValue.BulkString("message"),
            RespValue.BulkString("channel"),
            RespValue.BulkString("payload")
        });
        var result = await EncodeAsync(push, 2);
        Assert(result.StartsWith("*3\r\n", StringComparison.Ordinal), $"Expected *3 prefix in RESP2 degraded push, got: {result}");
    }

    // ─── Nested structures ────────────────────────────────────────────────────────

    [Test]
    public async Task Map_Resp3_MultipleEntries()
    {
        var map = RespValue.Map(new[]
        {
            RespValue.BulkString("a"), RespValue.IntegerValue(1),
            RespValue.BulkString("b"), RespValue.IntegerValue(2),
        });
        var result = await EncodeAsync(map, 3);
        AssertEqual("%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n", result);
    }

    [Test]
    public async Task Map_EmptyPairs_Resp3_EmitsPercentZero()
    {
        var map = RespValue.Map(Array.Empty<RespValue>());
        var result = await EncodeAsync(map, 3);
        AssertEqual("%0\r\n", result);
    }
}
