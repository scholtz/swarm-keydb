namespace SwarmKeyDb;

public enum OfflineOperationType
{
    Put,
    Delete
}

public sealed record OfflineJournalEntry(
    long Sequence,
    DateTimeOffset CreatedAtUtc,
    OfflineOperationType OperationType,
    string Key,
    byte[]? Value);

public interface IOfflineJournal
{
    Task<long> AppendAsync(OfflineOperationType operationType, string key, byte[]? value, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<OfflineJournalEntry>> ReadBatchAsync(int limit, CancellationToken cancellationToken = default);
    Task RemoveThroughAsync(long sequenceInclusive, CancellationToken cancellationToken = default);
    Task<long> CountAsync(CancellationToken cancellationToken = default);
}
