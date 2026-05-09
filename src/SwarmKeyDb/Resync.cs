using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public enum ResyncMode
{
    Auto,
    Partial,
    Full
}

public sealed class ResyncOptions
{
    public ResyncMode Mode { get; set; } = ResyncMode.Auto;
    public int MaxVersionGapForPartialResync { get; set; } = 128;
    public int FullResyncBatchSize { get; set; } = 256;
    public int ResyncTimeoutSeconds { get; set; } = 30;
}

public readonly record struct ResyncStatusSnapshot(
    bool InProgress,
    string CurrentMode,
    DateTimeOffset? LastResyncUtc,
    string LastMode,
    int KeysReplayedLastRun,
    long KeysReplayedTotal,
    string? LastError,
    string? CorrelationId);

public readonly record struct ResyncOperationResult(
    ResyncMode Mode,
    int KeysReplayed,
    long VersionGap,
    TimeSpan Duration,
    DateTimeOffset CompletedAtUtc);

public readonly record struct ResyncLifecycleContext(
    string CorrelationId,
    ResyncMode Mode,
    int KeysReplayed,
    long VersionGap,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    string? Error);

public interface IResyncStatusProvider
{
    ResyncStatusSnapshot GetSnapshot();
}

public interface IResyncMetricsReporter
{
    void RecordResync(ResyncMode mode, TimeSpan duration, int keysReplayed);
}

public sealed class NoOpResyncMetricsReporter : IResyncMetricsReporter
{
    public static readonly NoOpResyncMetricsReporter Instance = new();

    public void RecordResync(ResyncMode mode, TimeSpan duration, int keysReplayed)
    {
    }
}

public interface IResyncCoordinator : IResyncStatusProvider
{
    Action<ResyncLifecycleContext>? OnResyncStarted { get; set; }
    Action<ResyncLifecycleContext>? OnResyncCompleted { get; set; }
    Action<ResyncLifecycleContext>? OnResyncFailed { get; set; }
    Task<ResyncOperationResult> TriggerResyncAsync(ResyncMode mode = ResyncMode.Auto, CancellationToken cancellationToken = default);
}

public sealed class NoOpResyncCoordinator : IResyncCoordinator
{
    public static readonly NoOpResyncCoordinator Instance = new();
    public Action<ResyncLifecycleContext>? OnResyncStarted { get; set; }
    public Action<ResyncLifecycleContext>? OnResyncCompleted { get; set; }
    public Action<ResyncLifecycleContext>? OnResyncFailed { get; set; }

    public ResyncStatusSnapshot GetSnapshot() =>
        new(
            InProgress: false,
            CurrentMode: "idle",
            LastResyncUtc: null,
            LastMode: "none",
            KeysReplayedLastRun: 0,
            KeysReplayedTotal: 0,
            LastError: null,
            CorrelationId: null);

    public Task<ResyncOperationResult> TriggerResyncAsync(ResyncMode mode = ResyncMode.Auto, CancellationToken cancellationToken = default) =>
        Task.FromResult(new ResyncOperationResult(ResyncMode.Full, 0, 0, TimeSpan.Zero, DateTimeOffset.UtcNow));
}

public sealed class PartialResyncService
{
    private readonly ICacheSyncParticipant _participant;
    private readonly ICacheSyncPeerStateBus _peerStateBus;
    private readonly string _nodeId;
    private readonly ILogger<PartialResyncService> _logger;

    public PartialResyncService(
        ICacheSyncParticipant participant,
        ICacheSyncPeerStateBus peerStateBus,
        string nodeId,
        ILogger<PartialResyncService>? logger = null)
    {
        _participant = participant;
        _peerStateBus = peerStateBus;
        _nodeId = nodeId;
        _logger = logger ?? NullLogger<PartialResyncService>.Instance;
    }

    public async Task<PartialResyncResult> ReplayMissingDeltasAsync(int maxVersionGap, CancellationToken cancellationToken = default)
    {
        var local = await _participant.GetVersionStampsAsync(cancellationToken).ConfigureAwait(false);
        var peers = await _peerStateBus.GetPeerVersionStampsAsync(_nodeId, cancellationToken).ConfigureAwait(false);
        if (peers.Count == 0 || local.Count == 0)
        {
            return new PartialResyncResult(false, false, 0, 0);
        }

        var lastKnownStamp = local.Values.Max();
        var maxPeerStamp = peers.Values.SelectMany(static stamps => stamps.Values).DefaultIfEmpty(0).Max();
        var versionGap = Math.Max(0, maxPeerStamp - lastKnownStamp);
        if (versionGap > maxVersionGap)
        {
            return new PartialResyncResult(true, false, versionGap, 0);
        }

        var mergedPeerVersions = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var peer in peers.Values)
        {
            foreach (var entry in peer)
            {
                if (!mergedPeerVersions.TryGetValue(entry.Key, out var existing) || entry.Value > existing)
                {
                    mergedPeerVersions[entry.Key] = entry.Value;
                }
            }
        }

        var keysReplayed = 0;
        foreach (var entry in mergedPeerVersions.OrderBy(static pair => pair.Key, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var localVersion = local.TryGetValue(entry.Key, out var known) ? known : 0;
            if (entry.Value <= localVersion)
            {
                continue;
            }

            await _participant.ReconcileKeyAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
            keysReplayed++;
        }

        _logger.LogInformation(
            "Partial resync replay complete. node_id={node_id} keys_replayed={keys_replayed} version_gap={version_gap}",
            _nodeId,
            keysReplayed,
            versionGap);

        return new PartialResyncResult(true, true, versionGap, keysReplayed);
    }
}

public readonly record struct PartialResyncResult(
    bool HistoryAvailable,
    bool IsWithinGapThreshold,
    long VersionGap,
    int KeysReplayed);

public sealed class FullResyncService
{
    private readonly ICacheSyncParticipant _participant;
    private readonly IKeyValueStore _store;
    private readonly ILogger<FullResyncService> _logger;

    public FullResyncService(
        ICacheSyncParticipant participant,
        IKeyValueStore store,
        ILogger<FullResyncService>? logger = null)
    {
        _participant = participant;
        _store = store;
        _logger = logger ?? NullLogger<FullResyncService>.Instance;
    }

    public async Task<int> RebuildCacheAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        var normalizedBatchSize = Math.Max(1, batchSize);
        var keys = (await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false))
            .OrderBy(static key => key, StringComparer.Ordinal)
            .ToArray();
        var replayed = 0;
        for (var i = 0; i < keys.Length; i += normalizedBatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var chunk = keys.Skip(i).Take(normalizedBatchSize);
            foreach (var key in chunk)
            {
                await _participant.ReconcileKeyAsync(key, long.MaxValue, cancellationToken).ConfigureAwait(false);
                replayed++;
            }
        }

        _logger.LogInformation("Full resync rebuild complete. keys_replayed={keys_replayed}", replayed);
        return replayed;
    }
}

public sealed class ResyncCoordinator : IResyncCoordinator
{
    private readonly PartialResyncService? _partialResyncService;
    private readonly FullResyncService _fullResyncService;
    private readonly ResyncOptions _options;
    private readonly ILogger<ResyncCoordinator> _logger;
    private readonly IResyncMetricsReporter _metricsReporter;
    private readonly object _statusGate = new();
    private int _inProgress;
    private ResyncStatusSnapshot _snapshot = new(
        InProgress: false,
        CurrentMode: "idle",
        LastResyncUtc: null,
        LastMode: "none",
        KeysReplayedLastRun: 0,
        KeysReplayedTotal: 0,
        LastError: null,
        CorrelationId: null);

    public ResyncCoordinator(
        ICacheSyncParticipant participant,
        IKeyValueStore store,
        ICacheSyncBus syncBus,
        CacheSyncOptions cacheSyncOptions,
        ResyncOptions options,
        ILogger<ResyncCoordinator>? logger = null,
        IResyncMetricsReporter? metricsReporter = null)
    {
        _options = options;
        _logger = logger ?? NullLogger<ResyncCoordinator>.Instance;
        _metricsReporter = metricsReporter ?? NoOpResyncMetricsReporter.Instance;
        if (syncBus is ICacheSyncPeerStateBus peerStateBus)
        {
            _partialResyncService = new PartialResyncService(participant, peerStateBus, cacheSyncOptions.NodeId);
        }

        _fullResyncService = new FullResyncService(participant, store);
    }

    public Action<ResyncLifecycleContext>? OnResyncStarted { get; set; }
    public Action<ResyncLifecycleContext>? OnResyncCompleted { get; set; }
    public Action<ResyncLifecycleContext>? OnResyncFailed { get; set; }

    public ResyncStatusSnapshot GetSnapshot()
    {
        lock (_statusGate)
        {
            return _snapshot;
        }
    }

    public async Task<ResyncOperationResult> TriggerResyncAsync(ResyncMode mode = ResyncMode.Auto, CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _inProgress, 1, 0) != 0)
        {
            throw new InvalidOperationException("Resync is already in progress.");
        }

        var startedAt = DateTimeOffset.UtcNow;
        var correlationId = Guid.NewGuid().ToString("N");
        try
        {
            var requested = mode == ResyncMode.Auto ? _options.Mode : mode;
            SetSnapshot(_snapshot with
            {
                InProgress = true,
                CurrentMode = requested.ToString().ToLowerInvariant(),
                CorrelationId = correlationId,
                LastError = null
            });
            OnResyncStarted?.Invoke(new ResyncLifecycleContext(correlationId, requested, 0, 0, startedAt, null, null));

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(Math.Max(1, _options.ResyncTimeoutSeconds)));
            var token = timeoutCts.Token;
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var resolvedMode = requested;
            var versionGap = 0L;
            var keysReplayed = 0;

            if (requested is ResyncMode.Auto or ResyncMode.Partial)
            {
                var partialResult = _partialResyncService is null
                    ? new PartialResyncResult(false, false, 0, 0)
                    : await _partialResyncService.ReplayMissingDeltasAsync(_options.MaxVersionGapForPartialResync, token).ConfigureAwait(false);
                versionGap = partialResult.VersionGap;
                if (partialResult.HistoryAvailable && partialResult.IsWithinGapThreshold)
                {
                    resolvedMode = ResyncMode.Partial;
                    keysReplayed = partialResult.KeysReplayed;
                }
                else if (requested == ResyncMode.Partial)
                {
                    throw new InvalidOperationException("Partial resync is unavailable or version history is too stale.");
                }
                else
                {
                    resolvedMode = ResyncMode.Full;
                }
            }

            if (resolvedMode == ResyncMode.Full)
            {
                keysReplayed = await _fullResyncService.RebuildCacheAsync(_options.FullResyncBatchSize, token).ConfigureAwait(false);
            }

            stopwatch.Stop();
            var completedAt = DateTimeOffset.UtcNow;
            _metricsReporter.RecordResync(resolvedMode, stopwatch.Elapsed, keysReplayed);
            lock (_statusGate)
            {
                _snapshot = _snapshot with
                {
                    InProgress = false,
                    CurrentMode = "idle",
                    LastResyncUtc = completedAt,
                    LastMode = resolvedMode.ToString().ToLowerInvariant(),
                    KeysReplayedLastRun = keysReplayed,
                    KeysReplayedTotal = _snapshot.KeysReplayedTotal + keysReplayed,
                    LastError = null,
                    CorrelationId = correlationId
                };
            }

            var completionContext = new ResyncLifecycleContext(correlationId, resolvedMode, keysReplayed, versionGap, startedAt, completedAt, null);
            OnResyncCompleted?.Invoke(completionContext);
            return new ResyncOperationResult(resolvedMode, keysReplayed, versionGap, stopwatch.Elapsed, completedAt);
        }
        catch (Exception ex)
        {
            var completedAt = DateTimeOffset.UtcNow;
            lock (_statusGate)
            {
                _snapshot = _snapshot with
                {
                    InProgress = false,
                    CurrentMode = "idle",
                    LastError = ex.Message,
                    CorrelationId = correlationId
                };
            }

            OnResyncFailed?.Invoke(new ResyncLifecycleContext(correlationId, mode, 0, 0, startedAt, completedAt, ex.Message));
            _logger.LogWarning(ex, "Resync operation failed. correlation_id={correlation_id}", correlationId);
            throw;
        }
        finally
        {
            Interlocked.Exchange(ref _inProgress, 0);
        }
    }

    private void SetSnapshot(ResyncStatusSnapshot snapshot)
    {
        lock (_statusGate)
        {
            _snapshot = snapshot;
        }
    }
}
