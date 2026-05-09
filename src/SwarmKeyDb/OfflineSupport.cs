namespace SwarmKeyDb;

public sealed record OfflineWriteResult(bool Queued, long QueueDepth);

public sealed record OfflineReadResult(byte[]? Value, bool FromCache, DateTimeOffset? CachedAt);

public sealed record OfflineConflictContext(string Key, byte[]? LocalValue, byte[]? RemoteValue);

public interface IOfflineStatusProvider
{
    long QueueDepth { get; }
    DateTimeOffset? LastSuccessfulSyncUtc { get; }
    bool IsOffline { get; }
    OfflineMode Mode { get; }
}

public interface IOfflineKeyValueStore : IKeyValueStore, IOfflineStatusProvider
{
    Task<OfflineWriteResult> PutWithResultAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default);
    Task<OfflineWriteResult> DeleteWithResultAsync(string key, CancellationToken cancellationToken = default);
    Task<OfflineReadResult> GetWithResultAsync(string key, CancellationToken cancellationToken = default);
    Task<int> SyncPendingOperationsAsync(CancellationToken cancellationToken = default);
}

public sealed class NoOpOfflineStatusProvider : IOfflineStatusProvider
{
    public static NoOpOfflineStatusProvider Instance { get; } = new();

    public long QueueDepth => 0;
    public DateTimeOffset? LastSuccessfulSyncUtc => null;
    public bool IsOffline => false;
    public OfflineMode Mode => OfflineMode.Never;
}
