using System.Security.Cryptography;

namespace SwarmKeyDb;

public sealed class FileSwarmClient : ISwarmClient
{
    private readonly string _directory;

    public FileSwarmClient(string directory)
    {
        _directory = directory;
    }

    public async Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(_directory);
        var bytes = data.ToArray();
        var reference = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        var path = GetPath(reference);
        if (!File.Exists(path))
        {
            await File.WriteAllBytesAsync(path, bytes, cancellationToken).ConfigureAwait(false);
        }

        return reference;
    }

    public Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default)
    {
        var path = GetPath(reference);
        if (!File.Exists(path))
        {
            throw new KeyNotFoundException($"Swarm reference '{reference}' was not found.");
        }

        return File.ReadAllBytesAsync(path, cancellationToken);
    }

    private string GetPath(string reference) => Path.Combine(_directory, reference);
}
