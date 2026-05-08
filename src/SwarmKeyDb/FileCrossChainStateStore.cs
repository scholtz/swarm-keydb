using System.Text.Json;

namespace SwarmKeyDb;

public sealed class FileCrossChainStateStore : ICrossChainStateStore
{
    private readonly string _path;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public FileCrossChainStateStore(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        _path = path;
    }

    public async Task UpsertAsync(CrossChainSyncRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var records = await ReadAllAsync(cancellationToken).ConfigureAwait(false);
            records[record.Key] = record;
            await WriteAllAsync(records, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<CrossChainSyncRecord?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var records = await ReadAllAsync(cancellationToken).ConfigureAwait(false);
            return records.TryGetValue(key, out var record) ? record : null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<CrossChainSyncRecord>> ListAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var records = await ReadAllAsync(cancellationToken).ConfigureAwait(false);
            return records.Values.OrderBy(static record => record.Key, StringComparer.Ordinal).ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<Dictionary<string, CrossChainSyncRecord>> ReadAllAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_path))
        {
            return new Dictionary<string, CrossChainSyncRecord>(StringComparer.Ordinal);
        }

        await using var stream = File.OpenRead(_path);
        var records = await JsonSerializer.DeserializeAsync<Dictionary<string, CrossChainSyncRecord>>(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        return records is null
            ? new Dictionary<string, CrossChainSyncRecord>(StringComparer.Ordinal)
            : new Dictionary<string, CrossChainSyncRecord>(records, StringComparer.Ordinal);
    }

    private async Task WriteAllAsync(Dictionary<string, CrossChainSyncRecord> records, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var stream = File.Create(_path);
        await JsonSerializer.SerializeAsync(stream, records, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
