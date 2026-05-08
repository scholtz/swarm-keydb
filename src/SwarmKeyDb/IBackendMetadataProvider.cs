namespace SwarmKeyDb;

public interface IBackendMetadataProvider
{
    Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default);
}
