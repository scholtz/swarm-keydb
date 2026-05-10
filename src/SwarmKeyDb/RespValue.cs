using System.Globalization;
using System.Text;

namespace SwarmKeyDb;

public enum RespType
{
    // RESP2 types
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    // RESP3 types
    Map,
    Set,
    Double,
    Boolean,
    BigNumber,
    VerbatimString,
    Null,
    BlobError,
    Push
}

public sealed class RespValue
{
    private RespValue(RespType type, string? text = null, long integer = 0, byte[]? bytes = null,
        IReadOnlyList<RespValue>? items = null, double doubleValue = 0, bool boolValue = false)
    {
        Type = type;
        Text = text;
        Integer = integer;
        Bytes = bytes;
        Items = items;
        DoubleValue = doubleValue;
        BoolValue = boolValue;
    }

    public RespType Type { get; }
    public string? Text { get; }
    public long Integer { get; }
    public byte[]? Bytes { get; }
    public IReadOnlyList<RespValue>? Items { get; }
    public double DoubleValue { get; }
    public bool BoolValue { get; }

    // RESP2 types
    public static RespValue SimpleString(string value) => new(RespType.SimpleString, text: value);
    public static RespValue Error(string value) => new(RespType.Error, text: value);
    public static RespValue IntegerValue(long value) => new(RespType.Integer, integer: value);
    public static RespValue BulkString(byte[]? value) => new(RespType.BulkString, bytes: value);
    public static RespValue BulkString(string? value) => new(RespType.BulkString, bytes: value is null ? null : Encoding.UTF8.GetBytes(value));
    public static RespValue Array(IReadOnlyList<RespValue> items) => new(RespType.Array, items: items);
    public static RespValue NullArray() => new(RespType.Array);

    // RESP3 types
    /// <summary>
    /// RESP3 Map. Items must be an even-length list of alternating key-value pairs.
    /// In RESP2 mode this falls back to a flat array.
    /// </summary>
    public static RespValue Map(IReadOnlyList<RespValue> keyValuePairs) => new(RespType.Map, items: keyValuePairs);
    /// <summary>RESP3 Set. In RESP2 mode falls back to a plain array.</summary>
    public static RespValue Set(IReadOnlyList<RespValue> elements) => new(RespType.Set, items: elements);
    /// <summary>RESP3 Double. In RESP2 mode falls back to a bulk string.</summary>
    public static RespValue Double(double value) => new(RespType.Double, doubleValue: value);
    /// <summary>RESP3 Boolean. In RESP2 mode falls back to integer 1/0.</summary>
    public static RespValue Boolean(bool value) => new(RespType.Boolean, boolValue: value);
    /// <summary>RESP3 BigNumber. In RESP2 mode falls back to a bulk string.</summary>
    public static RespValue BigNumber(string digits) => new(RespType.BigNumber, text: digits);
    /// <summary>RESP3 VerbatimString. encoding must be exactly 3 chars (e.g. "txt", "mkd"). In RESP2 falls back to bulk string.</summary>
    public static RespValue VerbatimString(string encoding, string data) =>
        new(RespType.VerbatimString, text: encoding + ":" + data);
    /// <summary>RESP3 Null. In RESP2 mode falls back to $-1 (null bulk string).</summary>
    public static RespValue Null() => new(RespType.Null);
    /// <summary>RESP3 BlobError. In RESP2 mode falls back to a plain error.</summary>
    public static RespValue BlobError(string message) => new(RespType.BlobError, text: message);
    /// <summary>RESP3 Push type (out-of-band push message). Always encoded as &gt;count in RESP3; falls back to RESP2 array.</summary>
    public static RespValue Push(IReadOnlyList<RespValue> items) => new(RespType.Push, items: items);

    public string AsString() => Encoding.UTF8.GetString(Bytes ?? System.Array.Empty<byte>());
    public string AsDoubleString() => DoubleValue.ToString("G17", CultureInfo.InvariantCulture);
}
