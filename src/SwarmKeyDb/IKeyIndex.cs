namespace SwarmKeyDb;

public interface IKeyIndex
{
    Task<string?> GetReferenceAsync(string key, CancellationToken cancellationToken = default);
    Task SetReferenceAsync(string key, string reference, DateTimeOffset? expiresAt = null, CancellationToken cancellationToken = default);
    Task<bool> SetExpiryAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken = default);
    Task<(bool Exists, DateTimeOffset? ExpiresAt)> GetExpiryAsync(string key, CancellationToken cancellationToken = default);
    Task<bool> RemoveExpiryAsync(string key, CancellationToken cancellationToken = default);
    Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns keys within the given range in lexicographic order. Implementors can override
    /// this with an O(k) scan; the default falls back to <see cref="ListKeysAsync"/> + filter.
    /// </summary>
    async Task<IReadOnlyList<string>> GetKeysInRangeAsync(
        string? startKey,
        string? endKey,
        bool includeStart = true,
        bool includeEnd = true,
        CancellationToken cancellationToken = default)
    {
        var all = await ListKeysAsync(cancellationToken).ConfigureAwait(false);
        return all
            .Where(k => QueryScanHelpers.MatchesLowerBound(k, startKey, includeStart)
                     && QueryScanHelpers.MatchesUpperBound(k, endKey, includeEnd))
            .ToArray();
    }

    /// <summary>
    /// Rebuilds the in-memory index from the underlying storage. The default implementation
    /// is a no-op; persistent implementations should reload their state from durable storage.
    /// </summary>
    Task RebuildIndexAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}
