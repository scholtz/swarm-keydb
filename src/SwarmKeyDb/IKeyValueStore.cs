namespace SwarmKeyDb;

public interface IKeyValueStore
{
    Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default);
    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default);
}
