using System.Security.Cryptography;

namespace SwarmKeyDb;

public sealed class InMemorySwarmClient : ISwarmClient, ISwarmDeletionClient
{
    private readonly Dictionary<string, byte[]> _objects = new(StringComparer.Ordinal);

    public Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var bytes = data.ToArray();
        var reference = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        _objects[reference] = bytes;
        return Task.FromResult(reference);
    }

    public Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_objects.TryGetValue(reference, out var data))
        {
            throw new KeyNotFoundException($"Swarm reference '{reference}' was not found.");
        }

        return Task.FromResult(data.ToArray());
    }

    public Task<bool> DeleteAsync(string reference, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_objects.Remove(reference));
    }
}
