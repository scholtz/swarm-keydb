using System.Text;
using System.Text.Json;

namespace SwarmKeyDb;

public sealed class HybridSwarmClient : ISwarmClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly ISwarmClient _swarmClient;
    private readonly ISwarmClient _ipfsClient;
    private readonly int _maxRetries;

    public HybridSwarmClient(ISwarmClient swarmClient, ISwarmClient ipfsClient, int maxRetries = 1)
    {
        _swarmClient = swarmClient;
        _ipfsClient = ipfsClient;
        _maxRetries = Math.Max(0, maxRetries);
    }

    public async Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var swarmTask = UploadWithRetryAsync(_swarmClient, data, "swarm", cancellationToken);
        var ipfsTask = UploadWithRetryAsync(_ipfsClient, data, "ipfs", cancellationToken);
        await Task.WhenAll(swarmTask, ipfsTask).ConfigureAwait(false);

        var swarmResult = await swarmTask.ConfigureAwait(false);
        var ipfsResult = await ipfsTask.ConfigureAwait(false);
        if (swarmResult.Reference is null && ipfsResult.Reference is null)
        {
            throw new InvalidOperationException($"Hybrid write failed: swarm={swarmResult.ErrorMessage}; ipfs={ipfsResult.ErrorMessage}");
        }

        return HybridReferenceCodec.Encode(swarmResult.Reference, ipfsResult.Reference);
    }

    public async Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default)
    {
        if (!HybridReferenceCodec.TryDecode(reference, out var parsed))
        {
            return await DownloadFromEitherAsync(reference, null, cancellationToken).ConfigureAwait(false);
        }

        return await DownloadFromEitherAsync(parsed.SwarmReference, parsed.IpfsCid, cancellationToken).ConfigureAwait(false);
    }

    private async Task<byte[]> DownloadFromEitherAsync(string? swarmReference, string? ipfsCid, CancellationToken cancellationToken)
    {
        var candidates = new List<(string Name, Func<CancellationToken, Task<byte[]>> Download)>(2);
        if (!string.IsNullOrWhiteSpace(ipfsCid))
        {
            candidates.Add(("ipfs", ct => _ipfsClient.DownloadAsync(ipfsCid, ct)));
        }

        if (!string.IsNullOrWhiteSpace(swarmReference))
        {
            candidates.Add(("swarm", ct => _swarmClient.DownloadAsync(swarmReference, ct)));
        }

        if (candidates.Count == 0)
        {
            throw new KeyNotFoundException("Hybrid reference does not include any backend pointers.");
        }

        var pending = candidates
            .Select(candidate => TryDownloadAsync(candidate.Name, candidate.Download, cancellationToken))
            .ToList();

        var errors = new List<string>();
        while (pending.Count > 0)
        {
            var completed = await Task.WhenAny(pending).ConfigureAwait(false);
            pending.Remove(completed);
            var result = await completed.ConfigureAwait(false);
            if (result.Payload is not null)
            {
                return result.Payload;
            }

            errors.Add($"{result.Backend}:{result.ErrorMessage}");
        }

        throw new KeyNotFoundException($"Hybrid read failed on all backends ({string.Join(", ", errors)}).");
    }

    private async Task<(string? Reference, string? ErrorMessage)> UploadWithRetryAsync(
        ISwarmClient client,
        ReadOnlyMemory<byte> data,
        string backend,
        CancellationToken cancellationToken)
    {
        Exception? lastError = null;
        for (var attempt = 0; attempt <= _maxRetries; attempt++)
        {
            try
            {
                return (await client.UploadAsync(data, cancellationToken).ConfigureAwait(false), null);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lastError = ex;
            }
        }

        return (null, $"{backend} upload failed after {_maxRetries + 1} attempt(s): {lastError?.GetType().Name}: {lastError?.Message}");
    }

    private static async Task<(string Backend, byte[]? Payload, string? ErrorMessage)> TryDownloadAsync(
        string backend,
        Func<CancellationToken, Task<byte[]>> download,
        CancellationToken cancellationToken)
    {
        try
        {
            return (backend, await download(cancellationToken).ConfigureAwait(false), null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (backend, null, ex.Message);
        }
    }
}

public sealed record HybridReference(string? SwarmReference, string? IpfsCid);

public static class HybridReferenceCodec
{
    private const string Prefix = "hybrid:";
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public static string Encode(string? swarmReference, string? ipfsCid)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(new HybridReference(swarmReference, ipfsCid), JsonOptions);
        return Prefix + Convert.ToBase64String(payload);
    }

    public static bool TryDecode(string reference, out HybridReference decoded)
    {
        decoded = new HybridReference(null, null);
        if (!reference.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var encoded = reference[Prefix.Length..];
        try
        {
            var bytes = Convert.FromBase64String(encoded);
            decoded = JsonSerializer.Deserialize<HybridReference>(bytes, JsonOptions) ?? new HybridReference(null, null);
            return true;
        }
        catch (Exception ex) when (ex is FormatException or JsonException)
        {
            return false;
        }
    }
}
