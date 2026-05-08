using System.Text;

namespace SwarmKeyDb;

public sealed class RespWriter
{
    private static readonly byte[] CrLf = "\r\n"u8.ToArray();
    private readonly Stream _stream;

    public RespWriter(Stream stream)
    {
        _stream = stream;
    }

    public async Task WriteAsync(RespValue value, CancellationToken cancellationToken = default)
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
                await WriteArrayAsync(value.Items ?? Array.Empty<RespValue>(), cancellationToken).ConfigureAwait(false);
                break;
        }

        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task WriteArrayAsync(IReadOnlyList<RespValue> items, CancellationToken cancellationToken)
    {
        await WriteAsciiAsync($"*{items.Count}\r\n", cancellationToken).ConfigureAwait(false);
        foreach (var item in items)
        {
            await WriteAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task WriteBulkStringAsync(byte[]? bytes, CancellationToken cancellationToken)
    {
        if (bytes is null)
        {
            await WriteAsciiAsync("$-1\r\n", cancellationToken).ConfigureAwait(false);
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
