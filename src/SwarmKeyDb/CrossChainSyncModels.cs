namespace SwarmKeyDb;

public enum SyncState
{
    Pending,
    Synced,
    Failed
}

public sealed class CrossChainSyncRecord
{
    public string Key { get; set; } = string.Empty;
    public string Operation { get; set; } = "put";
    public string? ValueBase64 { get; set; }
    public DateTimeOffset UpdatedAtUtc { get; set; } = DateTimeOffset.UtcNow;
    public List<ChainSyncRecord> Chains { get; set; } = [];
}

public sealed class ChainSyncRecord
{
    public int ChainId { get; set; }
    public string ChainName { get; set; } = string.Empty;
    public string NamespacedKey { get; set; } = string.Empty;
    public SyncState State { get; set; } = SyncState.Pending;
    public int Attempts { get; set; }
    public string? LastError { get; set; }
    public DateTimeOffset? LastAttemptUtc { get; set; }
    public DateTimeOffset? NextRetryUtc { get; set; }
    public DateTimeOffset? SyncedAtUtc { get; set; }
}

public sealed record ChainSyncStatus(
    int ChainId,
    string ChainName,
    string NamespacedKey,
    string Status,
    int Attempts,
    string? LastError,
    DateTimeOffset? LastAttemptUtc,
    DateTimeOffset? NextRetryUtc,
    DateTimeOffset? SyncedAtUtc);

public sealed record CrossChainSyncStatus(string Key, string Operation, IReadOnlyList<ChainSyncStatus> Chains, DateTimeOffset UpdatedAtUtc);

public sealed record ChainSyncSummary(
    int ChainId,
    string ChainName,
    int PendingCount,
    int SyncedCount,
    int FailedCount,
    string Health);
