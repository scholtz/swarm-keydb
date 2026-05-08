using System.IO.Compression;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SwarmKeyDb;

public sealed class CompressingKeyValueStore : IKeyValueStore, IAccessControlVerifier
{
    // GZip magic bytes (RFC 1952)
    private static readonly byte[] GZipMagic = [0x1F, 0x8B];
    // Brotli has no standard magic bytes; we prepend a custom 2-byte marker (0xCE 0xB8)
    // so we can detect our own compressed Brotli blobs. This is a SwarmKeyDb-specific
    // wrapper format and is not compatible with standalone Brotli-encoded files.
    private static readonly byte[] BrotliMagic = [0xCE, 0xB8];

    private readonly IKeyValueStore _inner;
    private readonly CompressionOptions _options;
    private readonly ILogger<CompressingKeyValueStore> _logger;

    public CompressingKeyValueStore(
        IKeyValueStore inner,
        IOptions<CompressionOptions> options,
        ILogger<CompressingKeyValueStore> logger)
    {
        _inner = inner;
        _options = options.Value;
        _logger = logger;
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled || value.Length < _options.MinSizeBytes)
        {
            await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
            return;
        }

        var compressed = Compress(value.Span, _options.Algorithm);
        _logger.LogDebug(
            "Compressed key '{Key}': {OriginalSize} → {CompressedSize} bytes ({Ratio:P0})",
            key, value.Length, compressed.Length,
            compressed.Length < value.Length ? 1.0 - (double)compressed.Length / value.Length : 0.0);

        await _inner.PutAsync(key, compressed, cancellationToken).ConfigureAwait(false);
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        var data = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (data is null)
        {
            return null;
        }

        if (TryDecompress(data, out var decompressed))
        {
            return decompressed;
        }

        return data;
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(key, cancellationToken);

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _inner.ListKeysAsync(cancellationToken);

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        _inner.SetTtlAsync(key, ttl, cancellationToken);

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(key, cancellationToken);

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.RemoveTtlAsync(key, cancellationToken);

    public void EnsureReadAccess()
    {
        if (_inner is IAccessControlVerifier verifier)
        {
            verifier.EnsureReadAccess();
        }
    }

    public void EnsureWriteAccess()
    {
        if (_inner is IAccessControlVerifier verifier)
        {
            verifier.EnsureWriteAccess();
        }
    }

    private static byte[] Compress(ReadOnlySpan<byte> data, CompressionAlgorithm algorithm)
    {
        using var output = new MemoryStream();
        if (algorithm == CompressionAlgorithm.Brotli)
        {
            output.Write(BrotliMagic);
            using var brotli = new BrotliStream(output, CompressionLevel.Optimal, leaveOpen: true);
            brotli.Write(data);
        }
        else
        {
            // GZip streams already embed the 0x1F 0x8B magic header
            using var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true);
            gzip.Write(data);
        }

        return output.ToArray();
    }

    private static bool TryDecompress(byte[] data, out byte[] decompressed)
    {
        if (data.Length >= 2 && data[0] == GZipMagic[0] && data[1] == GZipMagic[1])
        {
            using var input = new MemoryStream(data);
            using var gzip = new GZipStream(input, CompressionMode.Decompress);
            using var result = new MemoryStream();
            gzip.CopyTo(result);
            decompressed = result.ToArray();
            return true;
        }

        if (data.Length >= 2 && data[0] == BrotliMagic[0] && data[1] == BrotliMagic[1])
        {
            using var input = new MemoryStream(data, 2, data.Length - 2);
            using var brotli = new BrotliStream(input, CompressionMode.Decompress);
            using var result = new MemoryStream();
            brotli.CopyTo(result);
            decompressed = result.ToArray();
            return true;
        }

        decompressed = Array.Empty<byte>();
        return false;
    }
}
