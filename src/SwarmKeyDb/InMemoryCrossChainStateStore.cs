namespace SwarmKeyDb;

public sealed class InMemoryCrossChainStateStore : ICrossChainStateStore
{
    private readonly Dictionary<string, CrossChainSyncRecord> _records = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();

    public Task UpsertAsync(CrossChainSyncRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        lock (_gate)
        {
            _records[record.Key] = Clone(record);
        }

        return Task.CompletedTask;
    }

    public Task<CrossChainSyncRecord?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        lock (_gate)
        {
            return Task.FromResult(_records.TryGetValue(key, out var record) ? Clone(record) : null);
        }
    }

    public Task<IReadOnlyList<CrossChainSyncRecord>> ListAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult<IReadOnlyList<CrossChainSyncRecord>>(_records.Values.Select(Clone).ToArray());
        }
    }

    private static CrossChainSyncRecord Clone(CrossChainSyncRecord record) =>
        new()
        {
            Key = record.Key,
            Operation = record.Operation,
            ValueBase64 = record.ValueBase64,
            UpdatedAtUtc = record.UpdatedAtUtc,
            Chains = record.Chains.Select(chain => new ChainSyncRecord
            {
                ChainId = chain.ChainId,
                ChainName = chain.ChainName,
                NamespacedKey = chain.NamespacedKey,
                State = chain.State,
                Attempts = chain.Attempts,
                LastError = chain.LastError,
                LastAttemptUtc = chain.LastAttemptUtc,
                NextRetryUtc = chain.NextRetryUtc,
                SyncedAtUtc = chain.SyncedAtUtc
            }).ToList()
        };
}
