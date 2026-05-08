namespace SwarmKeyDb;

public interface IKeyValueStore
{
    /// <summary>
    /// Stores the value for the given key. Implementations may apply CRDT merge semantics.
    /// </summary>
    Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores the value for the given key using an explicit merge strategy.
    /// </summary>
    Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default) =>
        PutAsync(key, value, cancellationToken);

    /// <summary>
    /// Merges an incoming value into an existing key using the configured merge strategy.
    /// </summary>
    Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        PutAsync(key, incomingValue, cancellationToken);

    /// <summary>
    /// Configures CRDT behavior for a specific key.
    /// </summary>
    Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default);
    Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.FromResult(false);
    Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult((false, (TimeSpan?)null));
    Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) => Task.FromResult(false);
}
