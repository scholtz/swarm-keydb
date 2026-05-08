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

    /// <summary>
    /// Returns all keys that begin with the provided prefix.
    /// </summary>
    /// <example>
    /// <code>
    /// var keys = await store.GetKeysWithPrefixAsync("user:alice:");
    /// </code>
    /// </example>
    async Task<IReadOnlyList<string>> GetKeysWithPrefixAsync(string prefix, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        var keys = await ListKeysAsync(cancellationToken).ConfigureAwait(false);
        return keys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)).ToArray();
    }

    /// <summary>
    /// Returns keys in lexicographic order between <paramref name="startKey"/> and <paramref name="endKey"/>.
    /// </summary>
    /// <example>
    /// <code>
    /// var items = await store.GetKeyRangeAsync("order:1000", "order:1999", new RangeScanOptions { IncludeValues = true });
    /// </code>
    /// </example>
    async Task<IReadOnlyList<RangeScanEntry>> GetKeyRangeAsync(
        string? startKey,
        string? endKey,
        RangeScanOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new RangeScanOptions();
        if (startKey is not null && endKey is not null && StringComparer.Ordinal.Compare(startKey, endKey) > 0)
        {
            throw new ArgumentException("startKey must be lexicographically ≤ endKey.", nameof(startKey));
        }

        if (options.Limit is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Limit must be greater than zero.");
        }

        var keys = await ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var filtered = keys.Where(key =>
            QueryScanHelpers.MatchesLowerBound(key, startKey, options.IncludeStart) &&
            QueryScanHelpers.MatchesUpperBound(key, endKey, options.IncludeEnd));

        filtered = options.Descending
            ? filtered.OrderByDescending(static key => key, StringComparer.Ordinal)
            : filtered.OrderBy(static key => key, StringComparer.Ordinal);

        if (options.Limit is { } limit)
        {
            filtered = filtered.Take(limit);
        }

        if (!options.IncludeValues)
        {
            return filtered.Select(static key => new RangeScanEntry(key, null)).ToArray();
        }

        var entries = new List<RangeScanEntry>();
        foreach (var key in filtered)
        {
            var value = await GetAsync(key, cancellationToken).ConfigureAwait(false);
            entries.Add(new RangeScanEntry(key, value));
        }

        return entries;
    }

    /// <summary>
    /// Streams keys and values matching predicates.
    /// </summary>
    /// <example>
    /// <code>
    /// await foreach (var item in store.QueryAsync(key => key.StartsWith("orders:"), value => value.Length &gt; 0))
    /// {
    ///     Console.WriteLine(item.Key);
    /// }
    /// </code>
    /// </example>
    async IAsyncEnumerable<KeyValuePair<string, byte[]>> QueryAsync(
        Func<string, bool> keyPredicate,
        Func<byte[], bool>? valuePredicate = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPredicate);
        var keys = await ListKeysAsync(cancellationToken).ConfigureAwait(false);
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!keyPredicate(key))
            {
                continue;
            }

            var value = await GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (value is null)
            {
                continue;
            }

            if (valuePredicate is not null && !valuePredicate(value))
            {
                continue;
            }

            yield return new KeyValuePair<string, byte[]>(key, value);
        }
    }

    /// <summary>
    /// Iterates keys using an opaque cursor.
    /// </summary>
    /// <example>
    /// <code>
    /// var page = await store.ScanAsync(null, 50);
    /// while (!string.IsNullOrEmpty(page.NextCursor))
    /// {
    ///     page = await store.ScanAsync(page.NextCursor, 50);
    /// }
    /// </code>
    /// </example>
    async Task<ScanResult> ScanAsync(string? cursor, int count, CancellationToken cancellationToken = default)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "count must be greater than zero.");
        }

        var keys = await ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var startIndex = QueryScanHelpers.DecodeCursor(cursor, keys.Count);
        var page = keys.Skip(startIndex).Take(count).ToArray();
        var nextIndex = startIndex + page.Length;
        var nextCursor = nextIndex >= keys.Count ? string.Empty : QueryScanHelpers.EncodeCursor(nextIndex);
        return new ScanResult(nextCursor, page);
    }

    Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.FromResult(false);
    Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult((false, (TimeSpan?)null));
    Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) => Task.FromResult(false);

}
