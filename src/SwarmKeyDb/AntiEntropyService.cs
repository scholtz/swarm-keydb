using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class AntiEntropyService : IAsyncDisposable, ICacheSyncStatusProvider
{
    private readonly ICacheSyncParticipant _participant;
    private readonly ICacheSyncBus _syncBus;
    private readonly CacheSyncOptions _options;
    private readonly ILogger<AntiEntropyService> _logger;
    private readonly TimeSpan _interval;
    private CancellationTokenSource? _cts;
    private Task? _loopTask;
    private long _pendingReconciliations;
    private int _peerCount;
    private int _reconciledKeysLastCycle;
    private string? _lastError;
    private long _lastSuccessfulSyncUnixMs;

    public AntiEntropyService(
        ICacheSyncParticipant participant,
        ICacheSyncBus syncBus,
        CacheSyncOptions options,
        ILogger<AntiEntropyService>? logger = null)
    {
        _participant = participant;
        _syncBus = syncBus;
        _options = options;
        _logger = logger ?? NullLogger<AntiEntropyService>.Instance;
        _interval = TimeSpan.FromSeconds(Math.Max(1, options.SyncIntervalSeconds));
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled || _syncBus is not ICacheSyncPeerStateBus || _loopTask is not null)
        {
            return Task.CompletedTask;
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loopTask = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public Task TriggerReconciliationAsync(CancellationToken cancellationToken = default) =>
        ReconcileAsync(cancellationToken);

    public CacheSyncSnapshot GetSnapshot() =>
        new(
            LastSuccessfulSyncUtc: ReadLastSuccessfulSyncUtc(),
            PeerCount: Volatile.Read(ref _peerCount),
            ReconciledKeysLastCycle: Volatile.Read(ref _reconciledKeysLastCycle),
            PendingReconciliations: Interlocked.Read(ref _pendingReconciliations) + _participant.PendingReconciliations,
            LastError: Volatile.Read(ref _lastError));

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await ReconcileAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await Task.Delay(_interval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task ReconcileAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled || _syncBus is not ICacheSyncPeerStateBus peerStateBus)
        {
            return;
        }

        var correlationId = Guid.NewGuid().ToString("N");
        try
        {
            var peers = await peerStateBus.GetPeerVersionStampsAsync(_options.NodeId, cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _peerCount, peerStateBus.GetConnectedPeerCount(_options.NodeId));

            var localVersions = await _participant.GetVersionStampsAsync(cancellationToken).ConfigureAwait(false);
            var reconciled = 0;
            var pending = 0L;

            foreach (var peer in peers)
            {
                var peerReconciled = 0;
                foreach (var pair in peer.Value)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (!localVersions.TryGetValue(pair.Key, out var localVersion) || pair.Value > localVersion)
                    {
                        pending++;
                        await _participant.ReconcileKeyAsync(pair.Key, pair.Value, cancellationToken).ConfigureAwait(false);
                        peerReconciled++;
                        reconciled++;
                    }
                }

                _logger.LogInformation(
                    "Cache anti-entropy reconciliation complete. correlation_id={correlation_id} peer={peer} key_count={key_count} sync_lag_ms={sync_lag_ms}",
                    correlationId,
                    peer.Key,
                    peerReconciled,
                    0);
            }

            Interlocked.Exchange(ref _pendingReconciliations, Math.Max(0, pending - reconciled));
            Volatile.Write(ref _reconciledKeysLastCycle, reconciled);
            Volatile.Write(ref _lastError, null);
            Interlocked.Exchange(ref _lastSuccessfulSyncUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Volatile.Write(ref _lastError, ex.Message);
            _logger.LogWarning(
                ex,
                "Cache anti-entropy reconciliation failed. correlation_id={correlation_id} peer={peer} key_count={key_count} sync_lag_ms={sync_lag_ms}",
                correlationId,
                "unknown",
                0,
                0);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is null)
        {
            return;
        }

        await _cts.CancelAsync().ConfigureAwait(false);
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

        _cts.Dispose();
    }

    private DateTimeOffset? ReadLastSuccessfulSyncUtc()
    {
        var unixMs = Interlocked.Read(ref _lastSuccessfulSyncUnixMs);
        return unixMs <= 0 ? null : DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
    }
}
