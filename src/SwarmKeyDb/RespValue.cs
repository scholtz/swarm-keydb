using System.Text;

namespace SwarmKeyDb;

public enum RespType
{
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array
}

public sealed class RespValue
{
    private RespValue(RespType type, string? text = null, long integer = 0, byte[]? bytes = null, IReadOnlyList<RespValue>? items = null)
    {
        Type = type;
        Text = text;
        Integer = integer;
        Bytes = bytes;
        Items = items;
    }

    public RespType Type { get; }
    public string? Text { get; }
    public long Integer { get; }
    public byte[]? Bytes { get; }
    public IReadOnlyList<RespValue>? Items { get; }

    public static RespValue SimpleString(string value) => new(RespType.SimpleString, text: value);
    public static RespValue Error(string value) => new(RespType.Error, text: value);
    public static RespValue IntegerValue(long value) => new(RespType.Integer, integer: value);
    public static RespValue BulkString(byte[]? value) => new(RespType.BulkString, bytes: value);
    public static RespValue BulkString(string? value) => new(RespType.BulkString, bytes: value is null ? null : Encoding.UTF8.GetBytes(value));
    public static RespValue Array(IReadOnlyList<RespValue> items) => new(RespType.Array, items: items);
    public static RespValue NullArray() => new(RespType.Array);

    public string AsString() => Encoding.UTF8.GetString(Bytes ?? System.Array.Empty<byte>());
}
