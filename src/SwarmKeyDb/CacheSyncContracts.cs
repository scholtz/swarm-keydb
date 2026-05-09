namespace SwarmKeyDb;

public sealed record CacheInvalidationEvent(
    string SourceNodeId,
    string Key,
    long VersionStamp,
    DateTimeOffset TimestampUtc,
    string Reason);

public interface ICacheSyncBus
{
    Task PublishInvalidationAsync(CacheInvalidationEvent invalidation, CancellationToken cancellationToken = default);
    IDisposable SubscribeInvalidations(Func<CacheInvalidationEvent, Task> handler);
}

public interface ICacheSyncBusWithNodeSubscriptions : ICacheSyncBus
{
    IDisposable SubscribeInvalidations(string nodeId, Func<CacheInvalidationEvent, Task> handler);
}

public interface ICacheSyncPeerStateBus
{
    IDisposable RegisterVersionProvider(string nodeId, Func<CancellationToken, Task<IReadOnlyDictionary<string, long>>> provider);
    Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, long>>> GetPeerVersionStampsAsync(string requesterNodeId, CancellationToken cancellationToken = default);
    int GetConnectedPeerCount(string requesterNodeId);
}

public interface ICacheSyncParticipant
{
    long PendingReconciliations { get; }
    Task<IReadOnlyDictionary<string, long>> GetVersionStampsAsync(CancellationToken cancellationToken = default);
    Task ReconcileKeyAsync(string key, long versionStamp, CancellationToken cancellationToken = default);
}

public interface ICacheSyncStatusProvider
{
    CacheSyncSnapshot GetSnapshot();
}

public readonly record struct CacheSyncSnapshot(
    DateTimeOffset? LastSuccessfulSyncUtc,
    int PeerCount,
    int ReconciledKeysLastCycle,
    long PendingReconciliations,
    string? LastError);

public sealed class NoOpCacheSyncBus : ICacheSyncBus
{
    public static readonly NoOpCacheSyncBus Instance = new();

    public Task PublishInvalidationAsync(CacheInvalidationEvent invalidation, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public IDisposable SubscribeInvalidations(Func<CacheInvalidationEvent, Task> handler) => NullSubscription.Instance;

    private sealed class NullSubscription : IDisposable
    {
        public static readonly NullSubscription Instance = new();
        public void Dispose()
        {
        }
    }
}

public sealed class NoOpCacheSyncStatusProvider : ICacheSyncStatusProvider
{
    public static readonly NoOpCacheSyncStatusProvider Instance = new();

    public CacheSyncSnapshot GetSnapshot() =>
        new(
            LastSuccessfulSyncUtc: null,
            PeerCount: 0,
            ReconciledKeysLastCycle: 0,
            PendingReconciliations: 0,
            LastError: null);
}
