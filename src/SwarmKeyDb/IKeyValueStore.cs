namespace SwarmKeyDb;

public interface IKeyValueStore
{
    Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default);
    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default);
    Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.FromResult(false);
    Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult((false, (TimeSpan?)null));
    Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) => Task.FromResult(false);
}
