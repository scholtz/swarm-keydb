namespace SwarmKeyDb;

public sealed class SwarmKeyValueStore : IKeyValueStore
{
    private readonly ISwarmClient _swarmClient;
    private readonly IKeyIndex _index;

    public SwarmKeyValueStore(ISwarmClient swarmClient, IKeyIndex index)
    {
        _swarmClient = swarmClient;
        _index = index;
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        var reference = await _swarmClient.UploadAsync(value, cancellationToken).ConfigureAwait(false);
        await _index.SetReferenceAsync(key, reference, expiresAt: null, cancellationToken).ConfigureAwait(false);
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        var reference = await _index.GetReferenceAsync(key, cancellationToken).ConfigureAwait(false);
        return reference is null
            ? null
            : await _swarmClient.DownloadAsync(reference, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        return await _index.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _index.ListKeysAsync(cancellationToken);

    public async Task<IReadOnlyList<string>> GetKeysWithPrefixAsync(string prefix, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        var keys = await _index.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        return keys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)).ToArray();
    }

    public async Task<IReadOnlyList<RangeScanEntry>> GetKeyRangeAsync(
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

        var keys = await _index.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var filtered = keys.Where(key =>
            (startKey is null || StringComparer.Ordinal.Compare(key, startKey) > 0 || (options.IncludeStart && key.Equals(startKey, StringComparison.Ordinal))) &&
            (endKey is null || StringComparer.Ordinal.Compare(key, endKey) < 0 || (options.IncludeEnd && key.Equals(endKey, StringComparison.Ordinal))));

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
            entries.Add(new RangeScanEntry(key, await GetAsync(key, cancellationToken).ConfigureAwait(false)));
        }

        return entries;
    }

    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> QueryAsync(
        Func<string, bool> keyPredicate,
        Func<byte[], bool>? valuePredicate = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPredicate);
        var keys = await _index.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!keyPredicate(key))
            {
                continue;
            }

            var value = await GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (value is null || (valuePredicate is not null && !valuePredicate(value)))
            {
                continue;
            }

            yield return new KeyValuePair<string, byte[]>(key, value);
        }
    }

    public async Task<ScanResult> ScanAsync(string? cursor, int count, CancellationToken cancellationToken = default)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "count must be greater than zero.");
        }

        var keys = await _index.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var startIndex = DecodeCursor(cursor, keys.Count);
        var page = keys.Skip(startIndex).Take(count).ToArray();
        var nextIndex = startIndex + page.Length;
        var nextCursor = nextIndex >= keys.Count ? string.Empty : EncodeCursor(nextIndex);
        return new ScanResult(nextCursor, page);
    }

    public async Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        if (ttl <= TimeSpan.Zero)
        {
            throw new ArgumentException("TTL must be greater than zero.", nameof(ttl));
        }

        return await _index.SetExpiryAsync(key, DateTimeOffset.UtcNow.Add(ttl), cancellationToken).ConfigureAwait(false);
    }

    public async Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        var (exists, expiresAt) = await _index.GetExpiryAsync(key, cancellationToken).ConfigureAwait(false);
        if (!exists)
        {
            return (false, null);
        }

        if (expiresAt is null)
        {
            return (true, null);
        }

        return (true, expiresAt.Value - DateTimeOffset.UtcNow);
    }

    public async Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        return await _index.RemoveExpiryAsync(key, cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentException("Keys must not be empty.", nameof(key));
        }
    }

    private static int DecodeCursor(string? cursor, int upperBound)
    {
        if (string.IsNullOrEmpty(cursor))
        {
            return 0;
        }

        try
        {
            var raw = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(cursor));
            if (!int.TryParse(raw, out var offset) || offset < 0 || offset > upperBound)
            {
                throw new ArgumentException("cursor is invalid.", nameof(cursor));
            }

            return offset;
        }
        catch (FormatException)
        {
            throw new ArgumentException("cursor is invalid.", nameof(cursor));
        }
    }

    private static string EncodeCursor(int value) =>
        Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
}
