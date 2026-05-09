using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class OfflineSyncService : IAsyncDisposable
{
    private readonly IOfflineKeyValueStore _store;
    private readonly TimeSpan _interval;
    private readonly ILogger<OfflineSyncService> _logger;
    private CancellationTokenSource? _cts;
    private Task? _loopTask;

    public OfflineSyncService(IOfflineKeyValueStore store, TimeSpan interval, ILogger<OfflineSyncService>? logger = null)
    {
        _store = store;
        _interval = interval <= TimeSpan.Zero ? TimeSpan.FromSeconds(5) : interval;
        _logger = logger ?? NullLogger<OfflineSyncService>.Instance;
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_loopTask is not null)
        {
            return Task.CompletedTask;
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loopTask = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task TriggerSyncAsync(CancellationToken cancellationToken = default) =>
        await _store.SyncPendingOperationsAsync(cancellationToken).ConfigureAwait(false);

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_store.QueueDepth > 0)
                {
                    await _store.SyncPendingOperationsAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Offline sync iteration failed.");
            }

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
}
