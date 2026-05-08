using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class CrossChainSyncService : IAsyncDisposable
{
    private readonly IReadOnlyDictionary<int, IChainAdapter> _adapters;
    private readonly ICrossChainStateStore _stateStore;
    private readonly CrossChainOptions _options;
    private readonly ILogger<CrossChainSyncService> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;

    public CrossChainSyncService(
        IEnumerable<IChainAdapter> adapters,
        ICrossChainStateStore stateStore,
        CrossChainOptions options,
        ILogger<CrossChainSyncService>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(adapters);
        ArgumentNullException.ThrowIfNull(stateStore);
        ArgumentNullException.ThrowIfNull(options);
        if (options.MaxRetryAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxRetryAttempts must be at least 1.");
        }

        _adapters = adapters.ToDictionary(static adapter => adapter.ChainId);
        _stateStore = stateStore;
        _options = options;
        _logger = logger ?? NullLogger<CrossChainSyncService>.Instance;
    }

    public bool Enabled => _options.Enabled;

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (!Enabled || _loopTask is not null)
        {
            return Task.CompletedTask;
        }

        _loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loopTask = Task.Run(() => ReconcileLoopAsync(_loopCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task PutAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IEnumerable<int>? chainIds = null,
        CancellationToken cancellationToken = default)
    {
        await SyncAsync(key, "put", value, chainIds, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteAsync(
        string key,
        IEnumerable<int>? chainIds = null,
        CancellationToken cancellationToken = default)
    {
        await SyncAsync(key, "delete", null, chainIds, cancellationToken).ConfigureAwait(false);
    }

    public async Task<CrossChainSyncStatus?> GetStatusAsync(string key, CancellationToken cancellationToken = default)
    {
        var record = await _stateStore.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return record is null ? null : ToStatus(record);
    }

    public async Task<IReadOnlyList<ChainSyncSummary>> GetSummaryAsync(CancellationToken cancellationToken = default)
    {
        var records = await _stateStore.ListAsync(cancellationToken).ConfigureAwait(false);
        var chainGroups = records
            .SelectMany(static record => record.Chains)
            .GroupBy(static chain => (chain.ChainId, chain.ChainName))
            .OrderBy(static group => group.Key.ChainId);

        return chainGroups
            .Select(static group =>
            {
                var pending = group.Count(static item => item.State == SyncState.Pending);
                var synced = group.Count(static item => item.State == SyncState.Synced);
                var failed = group.Count(static item => item.State == SyncState.Failed);
                var health = failed > 0 ? "red" : pending > 0 ? "yellow" : "green";
                return new ChainSyncSummary(group.Key.ChainId, group.Key.ChainName, pending, synced, failed, health);
            })
            .ToArray();
    }

    public async Task<bool> ForceSyncAsync(string key, CancellationToken cancellationToken = default)
    {
        var record = await _stateStore.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (record is null)
        {
            return false;
        }

        foreach (var chain in record.Chains)
        {
            if (chain.State == SyncState.Failed)
            {
                chain.State = SyncState.Pending;
                chain.NextRetryUtc = DateTimeOffset.UtcNow;
            }
        }

        await _stateStore.UpsertAsync(record, cancellationToken).ConfigureAwait(false);
        await ReconcileDueOperationsAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task ReconcileDueOperationsAsync(CancellationToken cancellationToken = default)
    {
        if (!Enabled)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var records = await _stateStore.ListAsync(cancellationToken).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            foreach (var record in records)
            {
                foreach (var chain in record.Chains.Where(static item => item.State == SyncState.Pending))
                {
                    if (chain.NextRetryUtc is { } nextRetryUtc && nextRetryUtc > now)
                    {
                        continue;
                    }

                    await AttemptChainAsync(record, chain, cancellationToken).ConfigureAwait(false);
                }

                record.UpdatedAtUtc = DateTimeOffset.UtcNow;
                await _stateStore.UpsertAsync(record, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task SyncAsync(
        string key,
        string operation,
        ReadOnlyMemory<byte>? value,
        IEnumerable<int>? chainIds,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        if (!Enabled)
        {
            return;
        }

        var resolvedChainIds = ResolveTargetChainIds(chainIds);
        if (resolvedChainIds.Count == 0)
        {
            return;
        }

        var record = new CrossChainSyncRecord
        {
            Key = key,
            Operation = operation,
            ValueBase64 = value.HasValue ? Convert.ToBase64String(value.Value.ToArray()) : null,
            UpdatedAtUtc = DateTimeOffset.UtcNow,
            Chains = resolvedChainIds.Select(chainId =>
            {
                var adapter = ResolveAdapter(chainId);
                return new ChainSyncRecord
                {
                    ChainId = adapter.ChainId,
                    ChainName = adapter.Name,
                    NamespacedKey = adapter.GetNamespacedKey(key),
                    State = SyncState.Pending
                };
            }).ToList()
        };

        await _stateStore.UpsertAsync(record, cancellationToken).ConfigureAwait(false);
        await ReconcileDueOperationsAsync(cancellationToken).ConfigureAwait(false);
    }

    private IReadOnlyList<int> ResolveTargetChainIds(IEnumerable<int>? chainIds)
    {
        var resolved = (chainIds ?? _options.DefaultChainIds)
            .Distinct()
            .Where(_adapters.ContainsKey)
            .ToArray();
        return resolved;
    }

    private IChainAdapter ResolveAdapter(int chainId) =>
        _adapters.TryGetValue(chainId, out var adapter)
            ? adapter
            : throw new InvalidOperationException($"No chain adapter is configured for chain {chainId}.");

    private async Task AttemptChainAsync(CrossChainSyncRecord record, ChainSyncRecord chain, CancellationToken cancellationToken)
    {
        var adapter = ResolveAdapter(chain.ChainId);
        chain.Attempts++;
        chain.LastAttemptUtc = DateTimeOffset.UtcNow;

        try
        {
            if (string.Equals(record.Operation, "delete", StringComparison.OrdinalIgnoreCase))
            {
                await adapter.DeleteAsync(record.Key, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var value = record.ValueBase64 is null
                    ? ReadOnlyMemory<byte>.Empty
                    : Convert.FromBase64String(record.ValueBase64);
                await adapter.PutAsync(record.Key, value, cancellationToken).ConfigureAwait(false);
            }

            chain.State = SyncState.Synced;
            chain.LastError = null;
            chain.NextRetryUtc = null;
            chain.SyncedAtUtc = DateTimeOffset.UtcNow;
        }
        catch (Exception ex)
        {
            var seconds = Math.Max(1, _options.RetryBaseDelaySeconds) * Math.Pow(2, Math.Max(0, chain.Attempts - 1));
            var delay = TimeSpan.FromSeconds(Math.Min(seconds, Math.Max(1, _options.MaxRetryDelaySeconds)));
            var canRetry = chain.Attempts < _options.MaxRetryAttempts;
            chain.State = canRetry ? SyncState.Pending : SyncState.Failed;
            chain.LastError = $"Failed to sync to chain {chain.ChainId} ({chain.ChainName}): {ex.Message}";
            chain.NextRetryUtc = canRetry ? DateTimeOffset.UtcNow.Add(delay) : null;
            _logger.LogWarning(ex, "Cross-chain sync failed for key {Key} on chain {ChainId}.", record.Key, chain.ChainId);
        }
    }

    private async Task ReconcileLoopAsync(CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(1, _options.ReconcileIntervalSeconds));
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await ReconcileDueOperationsAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Cross-chain reconciliation loop failed.");
            }

            try
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private static CrossChainSyncStatus ToStatus(CrossChainSyncRecord record) =>
        new(
            record.Key,
            record.Operation,
            record.Chains
                .OrderBy(static chain => chain.ChainId)
                .Select(static chain => new ChainSyncStatus(
                    chain.ChainId,
                    chain.ChainName,
                    chain.NamespacedKey,
                    chain.State.ToString().ToLowerInvariant(),
                    chain.Attempts,
                    chain.LastError,
                    chain.LastAttemptUtc,
                    chain.NextRetryUtc,
                    chain.SyncedAtUtc))
                .ToArray(),
            record.UpdatedAtUtc);

    public async ValueTask DisposeAsync()
    {
        if (_loopCts is not null)
        {
            await _loopCts.CancelAsync().ConfigureAwait(false);
            _loopCts.Dispose();
        }

        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _gate.Dispose();
    }
}
