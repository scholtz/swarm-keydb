namespace SwarmKeyDb;

public interface ISwarmClient
{
    Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
    Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default);
}
