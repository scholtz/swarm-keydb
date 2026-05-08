using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class SwarmKeyDbClient
{
    private readonly IKeyValueStore _store;
    private readonly ILogger<SwarmKeyDbClient> _logger;

    public SwarmKeyDbClient(IKeyValueStore store, ILogger<SwarmKeyDbClient>? logger = null)
    {
        _store = store;
        _logger = logger ?? NullLogger<SwarmKeyDbClient>.Instance;
    }

    public Task PutBytesAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(key, value, cancellationToken);

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        PutBytesAsync(key, value, cancellationToken);

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

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        GetBytesAsync(key, cancellationToken);

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

    public async Task<IReadOnlyList<byte[]?>> BatchGetAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var tasks = keys.Select(key => _store.GetAsync(key, cancellationToken)).ToArray();
        return await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public async Task BatchPutAsync(
        IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>> values,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(values);
        var tasks = values.Select(entry => _store.PutAsync(entry.Key, entry.Value, cancellationToken)).ToArray();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

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

    /// <summary>
    /// Returns a scoped <see cref="SwarmKeyDbClient"/> where all operations are implicitly prefixed with
    /// <paramref name="prefix"/>. Listed keys have the prefix stripped so callers only see relative key names.
    /// </summary>
    /// <example>
    /// <code>
    /// var userDb = client.WithNamespace("users:alice:");
    /// await userDb.PutStringAsync("profile", json);
    /// var keys = await userDb.KeysAsync(); // ["profile"]
    /// </code>
    /// </example>
    public SwarmKeyDbClient WithNamespace(string prefix) =>
        new SwarmKeyDbClient(new NamespacedKeyValueStore(_store, prefix));

    /// <summary>
    /// Deletes all keys that start with <paramref name="prefix"/>.
    /// Returns the number of keys deleted.
    /// </summary>
    public Task<int> DeleteNamespaceAsync(string prefix, CancellationToken cancellationToken = default) =>
        _store.DeleteNamespaceAsync(prefix, cancellationToken);

    public Task FlushAsync(CancellationToken cancellationToken = default) =>
        _store is IAsyncProcessingStore asyncStore
            ? asyncStore.FlushAsync(cancellationToken)
            : Task.CompletedTask;

    public void FireAndForget(Func<Task> operation, string operationName = "fire-and-forget")
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (_store is IAsyncProcessingStore asyncStore)
        {
            asyncStore.FireAndForget(operation, operationName);
            return;
        }

        try
        {
            var task = operation();
            if (task.IsCompleted)
            {
                if (task.IsFaulted)
                {
                    LogFireAndForgetFailure(task.Exception?.GetBaseException(), operationName);
                }

                return;
            }

            _ = task.ContinueWith(
                continuationTask =>
                {
                    LogFireAndForgetFailure(continuationTask.Exception?.GetBaseException(), operationName);
                },
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            LogFireAndForgetFailure(ex, operationName);
        }
    }

    public void FireAndForget(Action operation, string operationName = "fire-and-forget")
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (_store is IAsyncProcessingStore asyncStore)
        {
            asyncStore.FireAndForget(operation, operationName);
            return;
        }

        _ = Task.Run(() =>
        {
            try
            {
                operation();
            }
            catch (Exception ex)
            {
                LogFireAndForgetFailure(ex, operationName);
            }
        });
    }

    private void LogFireAndForgetFailure(Exception? exception, string operationName)
    {
        _logger.LogError(exception, "Fire-and-forget operation '{OperationName}' failed.", operationName);
    }
}
