using System.Text;

namespace SwarmKeyDb;

public sealed class RespReader
{
    private readonly Stream _stream;

    public RespReader(Stream stream)
    {
        _stream = stream;
    }

    public async Task<RespValue?> ReadAsync(CancellationToken cancellationToken = default)
    {
        var first = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
        if (first < 0)
        {
            return null;
        }

        return first switch
        {
            (byte)'*' => await ReadArrayAsync(cancellationToken).ConfigureAwait(false),
            (byte)'$' => await ReadBulkStringAsync(cancellationToken).ConfigureAwait(false),
            (byte)'+' => RespValue.BulkString(await ReadLineAsync(cancellationToken).ConfigureAwait(false)),
            (byte)':' => RespValue.IntegerValue(long.Parse(await ReadLineAsync(cancellationToken).ConfigureAwait(false))),
            _ => await ReadInlineCommandAsync((byte)first, cancellationToken).ConfigureAwait(false)
        };
    }

    private async Task<RespValue> ReadArrayAsync(CancellationToken cancellationToken)
    {
        var count = int.Parse(await ReadLineAsync(cancellationToken).ConfigureAwait(false));
        var values = new List<RespValue>(count);
        for (var i = 0; i < count; i++)
        {
            var value = await ReadAsync(cancellationToken).ConfigureAwait(false)
                ?? throw new EndOfStreamException("Unexpected end of RESP array.");
            values.Add(value);
        }

        return RespValue.Array(values);
    }

    private async Task<RespValue> ReadBulkStringAsync(CancellationToken cancellationToken)
    {
        var length = int.Parse(await ReadLineAsync(cancellationToken).ConfigureAwait(false));
        if (length < 0)
        {
            return RespValue.BulkString((byte[]?)null);
        }

        var buffer = new byte[length];
        var offset = 0;
        while (offset < length)
        {
            var read = await _stream.ReadAsync(buffer.AsMemory(offset, length - offset), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                throw new EndOfStreamException("Unexpected end of RESP bulk string.");
            }

            offset += read;
        }

        var cr = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
        var lf = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
        if (cr != '\r' || lf != '\n')
        {
            throw new InvalidDataException("RESP bulk string was not terminated with CRLF.");
        }

        return RespValue.BulkString(buffer);
    }

    private async Task<RespValue> ReadInlineCommandAsync(byte first, CancellationToken cancellationToken)
    {
        var line = (char)first + await ReadLineAsync(cancellationToken).ConfigureAwait(false);
        var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(RespValue.BulkString)
            .ToArray();
        return RespValue.Array(parts);
    }

    private async Task<string> ReadLineAsync(CancellationToken cancellationToken)
    {
        var buffer = new List<byte>();
        while (true)
        {
            var value = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
            if (value < 0)
            {
                throw new EndOfStreamException("Unexpected end of RESP line.");
            }

            if (value == '\r')
            {
                var lf = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
                if (lf != '\n')
                {
                    throw new InvalidDataException("RESP line was not terminated with LF.");
                }

                return Encoding.UTF8.GetString(buffer.ToArray());
            }

            buffer.Add((byte)value);
        }
    }

    private async Task<int> ReadByteAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[1];
        var read = await _stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        return read == 0 ? -1 : buffer[0];
    }
}
