using System.Text;
using System.Text.Json;

namespace SwarmKeyDb;

public sealed class SwarmKeyDbClient
{
    private readonly IKeyValueStore _store;

    public SwarmKeyDbClient(IKeyValueStore store)
    {
        _store = store;
    }

    public Task PutBytesAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(key, value, cancellationToken);

    /// <summary>
    /// Stores a value using an explicit merge strategy for this write.
    /// </summary>
    public Task PutBytesWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default) =>
        _store.PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken);

    /// <summary>
    /// Merges an incoming value into an existing key using the configured strategy.
    /// </summary>
    public Task MergeBytesAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        _store.MergeAsync(key, incomingValue, cancellationToken);

    /// <summary>
    /// Configures per-key CRDT options such as merge strategy.
    /// </summary>
    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        _store.SetKeyOptionsAsync(key, options, cancellationToken);

    public Task<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default) =>
        _store.GetAsync(key, cancellationToken);

    public Task PutStringAsync(string key, string value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(key, Encoding.UTF8.GetBytes(value), cancellationToken);

    public Task MergeStringAsync(string key, string incomingValue, CancellationToken cancellationToken = default) =>
        _store.MergeAsync(key, Encoding.UTF8.GetBytes(incomingValue), cancellationToken);

    public async Task<string?> GetStringAsync(string key, CancellationToken cancellationToken = default)
    {
        var bytes = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    public Task PutJsonAsync<T>(string key, T value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(key, JsonSerializer.SerializeToUtf8Bytes(value), cancellationToken);

    public async Task<T?> GetJsonAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        var bytes = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return bytes is null ? default : JsonSerializer.Deserialize<T>(bytes);
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _store.DeleteAsync(key, cancellationToken);

    public Task<IReadOnlyList<string>> KeysAsync(CancellationToken cancellationToken = default) =>
        _store.ListKeysAsync(cancellationToken);

    public Task<IReadOnlyList<string>> GetKeysWithPrefixAsync(string prefix, CancellationToken cancellationToken = default) =>
        _store.GetKeysWithPrefixAsync(prefix, cancellationToken);

    public Task<IReadOnlyList<RangeScanEntry>> GetKeyRangeAsync(
        string? startKey,
        string? endKey,
        RangeScanOptions? options = null,
        CancellationToken cancellationToken = default) =>
        _store.GetKeyRangeAsync(startKey, endKey, options, cancellationToken);

    public IAsyncEnumerable<KeyValuePair<string, byte[]>> QueryAsync(
        Func<string, bool> keyPredicate,
        Func<byte[], bool>? valuePredicate = null,
        CancellationToken cancellationToken = default) =>
        _store.QueryAsync(keyPredicate, valuePredicate, cancellationToken);

    public Task<ScanResult> ScanAsync(string? cursor, int count, CancellationToken cancellationToken = default) =>
        _store.ScanAsync(cursor, count, cancellationToken);
}
