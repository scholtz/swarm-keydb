namespace SwarmKeyDb;

public sealed class CrossChainReplicatingKeyValueStore : IKeyValueStore
{
    private readonly IKeyValueStore _inner;
    private readonly CrossChainSyncService _syncService;
    private readonly IReadOnlyList<int>? _defaultChainIds;

    public CrossChainReplicatingKeyValueStore(
        IKeyValueStore inner,
        CrossChainSyncService syncService,
        IEnumerable<int>? defaultChainIds = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(syncService);
        _inner = inner;
        _syncService = syncService;
        _defaultChainIds = defaultChainIds?.Distinct().ToArray();
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
        await _syncService.PutAsync(key, value, _defaultChainIds, cancellationToken).ConfigureAwait(false);
    }

    public Task PutWithStrategyAsync(string key, ReadOnlyMemory<byte> value, IMergeStrategy mergeStrategy, CancellationToken cancellationToken = default) =>
        _inner.PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken);

    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        _inner.MergeAsync(key, incomingValue, cancellationToken);

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        _inner.SetKeyOptionsAsync(key, options, cancellationToken);

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetAsync(key, cancellationToken);

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        var deleted = await _inner.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
        if (deleted)
        {
            await _syncService.DeleteAsync(key, _defaultChainIds, cancellationToken).ConfigureAwait(false);
        }

        return deleted;
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _inner.ListKeysAsync(cancellationToken);

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        _inner.SetTtlAsync(key, ttl, cancellationToken);

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(key, cancellationToken);

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.RemoveTtlAsync(key, cancellationToken);
}
