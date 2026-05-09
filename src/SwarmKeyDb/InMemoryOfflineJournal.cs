using System.Collections.Concurrent;

namespace SwarmKeyDb;

public sealed class InMemoryOfflineJournal : IOfflineJournal
{
    private readonly ConcurrentQueue<OfflineJournalEntry> _entries = new();
    private long _nextSequence;

    public Task<long> AppendAsync(OfflineOperationType operationType, string key, byte[]? value, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var sequence = Interlocked.Increment(ref _nextSequence);
        _entries.Enqueue(new OfflineJournalEntry(sequence, DateTimeOffset.UtcNow, operationType, key, value?.ToArray()));
        return Task.FromResult(sequence);
    }

    public Task<IReadOnlyList<OfflineJournalEntry>> ReadBatchAsync(int limit, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (limit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), "limit must be greater than zero.");
        }

        return Task.FromResult<IReadOnlyList<OfflineJournalEntry>>(_entries.Take(limit).ToArray());
    }

    public Task RemoveThroughAsync(long sequenceInclusive, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        while (_entries.TryPeek(out var entry) && entry.Sequence <= sequenceInclusive)
        {
            _entries.TryDequeue(out _);
        }

        return Task.CompletedTask;
    }

    public Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult((long)_entries.Count);
    }
}
