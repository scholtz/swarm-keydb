namespace SwarmKeyDb;

/// <summary>
/// A sorted Red-Black tree index backed by <see cref="SortedDictionary{TKey,TValue}"/>.
/// All operations are thread-safe via an internal lock.
/// <list type="bullet">
///   <item>Single-key lookup / insert / delete: O(log n)</item>
///   <item>Range scan returning k results: O(log n + k)</item>
///   <item>Prefix scan returning k results: O(log n + k)</item>
///   <item>Full key enumeration: O(n) — already in sorted order, no additional sort needed</item>
/// </list>
/// </summary>
public sealed class BTreeKeyIndex : IKeyIndex
{
    private readonly SortedDictionary<string, KeyIndexEntry> _tree =
        new(StringComparer.Ordinal);

    private readonly object _lock = new();

    // ── IKeyIndex implementation ─────────────────────────────────────────────

    public Task<string?> GetReferenceAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            return Task.FromResult(TryGetActive(key, DateTimeOffset.UtcNow, out var entry) ? entry.Reference : null);
        }
    }

    public Task SetReferenceAsync(string key, string reference, DateTimeOffset? expiresAt = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            _tree[key] = new KeyIndexEntry(reference, expiresAt);
        }

        return Task.CompletedTask;
    }

    public Task<bool> SetExpiryAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetActive(key, DateTimeOffset.UtcNow, out var entry))
            {
                return Task.FromResult(false);
            }

            _tree[key] = entry with { ExpiresAt = expiresAt };
            return Task.FromResult(true);
        }
    }

    public Task<(bool Exists, DateTimeOffset? ExpiresAt)> GetExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetActive(key, DateTimeOffset.UtcNow, out var entry))
            {
                return Task.FromResult((false, (DateTimeOffset?)null));
            }

            return Task.FromResult((true, entry.ExpiresAt));
        }
    }

    public Task<bool> RemoveExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetActive(key, DateTimeOffset.UtcNow, out var entry) || entry.ExpiresAt is null)
            {
                return Task.FromResult(false);
            }

            _tree[key] = entry with { ExpiresAt = null };
            return Task.FromResult(true);
        }
    }

    public Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            return Task.FromResult(_tree.Remove(key));
        }
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            PurgeExpired(DateTimeOffset.UtcNow);
            return Task.FromResult<IReadOnlyList<string>>(_tree.Keys.ToArray());
        }
    }

    /// <summary>
    /// O(log n + k) range scan. Iterates the sorted tree starting from the first key that
    /// satisfies the lower bound and stops as soon as the upper bound is exceeded.
    /// </summary>
    public Task<IReadOnlyList<string>> GetKeysInRangeAsync(
        string? startKey,
        string? endKey,
        bool includeStart = true,
        bool includeEnd = true,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var now = DateTimeOffset.UtcNow;
            PurgeExpired(now);

            var result = new List<string>();
            foreach (var (key, _) in _tree)
            {
                if (!QueryScanHelpers.MatchesLowerBound(key, startKey, includeStart))
                {
                    continue;
                }

                if (!QueryScanHelpers.MatchesUpperBound(key, endKey, includeEnd))
                {
                    break;
                }

                result.Add(key);
            }

            return Task.FromResult<IReadOnlyList<string>>(result);
        }
    }

    /// <inheritdoc/>
    public Task RebuildIndexAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            PurgeExpired(DateTimeOffset.UtcNow);
        }

        return Task.CompletedTask;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private bool TryGetActive(string key, DateTimeOffset now, out KeyIndexEntry entry)
    {
        if (!_tree.TryGetValue(key, out var value))
        {
            entry = default;
            return false;
        }

        entry = value;
        if (entry.ExpiresAt is { } expiresAt && expiresAt <= now)
        {
            _tree.Remove(key);
            return false;
        }

        return true;
    }

    private void PurgeExpired(DateTimeOffset now)
    {
        var expired = _tree
            .Where(p => p.Value.ExpiresAt is { } exp && exp <= now)
            .Select(p => p.Key)
            .ToArray();

        foreach (var key in expired)
        {
            _tree.Remove(key);
        }
    }

    private readonly record struct KeyIndexEntry(string Reference, DateTimeOffset? ExpiresAt);
}
