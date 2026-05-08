namespace SwarmKeyDb;

public sealed class IpfsStorageBackend : IKeyValueStore, IBackendMetadataProvider
{
    private readonly SwarmKeyValueStore _inner;

    public IpfsStorageBackend(ISwarmClient ipfsClient, IKeyIndex index, IntegrityOptions? integrityOptions = null)
    {
        _inner = new SwarmKeyValueStore(ipfsClient, index, integrityOptions);
    }

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _inner.PutAsync(key, value, cancellationToken);

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetAsync(key, cancellationToken);

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

    public Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetBackendMetadataAsync(key, cancellationToken);
}
