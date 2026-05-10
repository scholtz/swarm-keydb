using System.Text;

namespace SwarmKeyDb;

public sealed class RespWriter
{
    private static readonly byte[] CrLf = "\r\n"u8.ToArray();

    /// <summary>Minimum number of bytes in a VerbatimString payload: 3-char encoding + ':' = 4.</summary>
    private const int MinVerbatimStringLength = 4;

    private readonly Stream _stream;

    /// <summary>Negotiated RESP protocol version. 2 = RESP2 (default), 3 = RESP3.</summary>
    public int ProtocolVersion { get; set; } = 2;

    public RespWriter(Stream stream)
    {
        _stream = stream;
    }

    public async Task WriteAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        await WriteValueAsync(value, cancellationToken).ConfigureAwait(false);
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task WriteValueAsync(RespValue value, CancellationToken cancellationToken)
    {
        switch (value.Type)
        {
            case RespType.SimpleString:
                await WriteAsciiAsync($"+{value.Text}\r\n", cancellationToken).ConfigureAwait(false);
                break;

            case RespType.Error:
                await WriteAsciiAsync($"-{value.Text}\r\n", cancellationToken).ConfigureAwait(false);
                break;

            case RespType.Integer:
                await WriteAsciiAsync($":{value.Integer}\r\n", cancellationToken).ConfigureAwait(false);
                break;

            case RespType.BulkString:
                await WriteBulkStringAsync(value.Bytes, cancellationToken).ConfigureAwait(false);
                break;

            case RespType.Array:
                if (value.Items is null)
                {
                    await WriteAsciiAsync("*-1\r\n", cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteCollectionAsync('*', value.Items, cancellationToken).ConfigureAwait(false);
                }
                break;

            // RESP3 types — degrade gracefully to RESP2 equivalents when ProtocolVersion < 3
            case RespType.Map:
                if (ProtocolVersion >= 3)
                {
                    var pairs = (value.Items ?? []).Count / 2;
                    await WriteAsciiAsync($"%{pairs}\r\n", cancellationToken).ConfigureAwait(false);
                    foreach (var item in value.Items ?? [])
                    {
                        await WriteValueAsync(item, cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    // Flat array of key/value pairs
                    await WriteCollectionAsync('*', value.Items ?? [], cancellationToken).ConfigureAwait(false);
                }
                break;

            case RespType.Set:
                if (ProtocolVersion >= 3)
                {
                    await WriteCollectionAsync('~', value.Items ?? [], cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteCollectionAsync('*', value.Items ?? [], cancellationToken).ConfigureAwait(false);
                }
                break;

            case RespType.Double:
                if (ProtocolVersion >= 3)
                {
                    await WriteAsciiAsync($",{value.AsDoubleString()}\r\n", cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteBulkStringAsync(System.Text.Encoding.ASCII.GetBytes(value.AsDoubleString()), cancellationToken).ConfigureAwait(false);
                }
                break;

            case RespType.Boolean:
                if (ProtocolVersion >= 3)
                {
                    await WriteAsciiAsync(value.BoolValue ? "#t\r\n" : "#f\r\n", cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteAsciiAsync(value.BoolValue ? ":1\r\n" : ":0\r\n", cancellationToken).ConfigureAwait(false);
                }
                break;

            case RespType.BigNumber:
                if (ProtocolVersion >= 3)
                {
                    await WriteAsciiAsync($"({value.Text}\r\n", cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteBulkStringAsync(value.Text is null ? null : Encoding.UTF8.GetBytes(value.Text), cancellationToken).ConfigureAwait(false);
                }
                break;

            case RespType.VerbatimString:
                if (ProtocolVersion >= 3 && value.Text is { Length: >= MinVerbatimStringLength })
                {
                    var rawBytes = Encoding.UTF8.GetBytes(value.Text);
                    await WriteAsciiAsync($"={rawBytes.Length}\r\n", cancellationToken).ConfigureAwait(false);
                    await _stream.WriteAsync(rawBytes, cancellationToken).ConfigureAwait(false);
                    await _stream.WriteAsync(CrLf, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var data = value.Text;
                    if (data is { Length: >= MinVerbatimStringLength })
                    {
                        var colonIndex = data.IndexOf(':', StringComparison.Ordinal);
                        data = colonIndex >= 0 ? data[(colonIndex + 1)..] : data;
                    }

                    await WriteBulkStringAsync(data is null ? null : Encoding.UTF8.GetBytes(data), cancellationToken).ConfigureAwait(false);
                }
                break;

            case RespType.Null:
                await WriteAsciiAsync(ProtocolVersion >= 3 ? "_\r\n" : "$-1\r\n", cancellationToken).ConfigureAwait(false);
                break;

            case RespType.BlobError:
                if (ProtocolVersion >= 3 && value.Text is not null)
                {
                    var errorBytes = Encoding.UTF8.GetBytes(value.Text);
                    await WriteAsciiAsync($"!{errorBytes.Length}\r\n", cancellationToken).ConfigureAwait(false);
                    await _stream.WriteAsync(errorBytes, cancellationToken).ConfigureAwait(false);
                    await _stream.WriteAsync(CrLf, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteAsciiAsync($"-{value.Text}\r\n", cancellationToken).ConfigureAwait(false);
                }
                break;

            case RespType.Push:
                if (ProtocolVersion >= 3)
                {
                    await WriteCollectionAsync('>', value.Items ?? [], cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteCollectionAsync('*', value.Items ?? [], cancellationToken).ConfigureAwait(false);
                }
                break;
        }
    }

    private async Task WriteCollectionAsync(char prefix, IReadOnlyList<RespValue> items, CancellationToken cancellationToken)
    {
        await WriteAsciiAsync($"{prefix}{items.Count}\r\n", cancellationToken).ConfigureAwait(false);
        foreach (var item in items)
        {
            await WriteValueAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task WriteBulkStringAsync(byte[]? bytes, CancellationToken cancellationToken)
    {
        if (bytes is null)
        {
            await WriteAsciiAsync(ProtocolVersion >= 3 ? "_\r\n" : "$-1\r\n", cancellationToken).ConfigureAwait(false);
            return;
        }

        await WriteAsciiAsync($"${bytes.Length}\r\n", cancellationToken).ConfigureAwait(false);
        await _stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        await _stream.WriteAsync(CrLf, cancellationToken).ConfigureAwait(false);
    }

    private Task WriteAsciiAsync(string text, CancellationToken cancellationToken)
    {
        var bytes = Encoding.ASCII.GetBytes(text);
        return _stream.WriteAsync(bytes, cancellationToken).AsTask();
    }
}
