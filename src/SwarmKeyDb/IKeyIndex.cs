namespace SwarmKeyDb;

public interface IKeyIndex
{
    Task<string?> GetReferenceAsync(string key, CancellationToken cancellationToken = default);
    Task SetReferenceAsync(string key, string reference, CancellationToken cancellationToken = default);
    Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default);
}
