namespace SwarmKeyDb;

/// <summary>
/// Immutable version vector used to detect causal ordering between writes.
/// </summary>
public sealed class VectorClock
{
    private readonly Dictionary<string, long> _entries;

    public static VectorClock Empty { get; } = new(new Dictionary<string, long>(StringComparer.Ordinal));

    public VectorClock(IReadOnlyDictionary<string, long> entries)
    {
        _entries = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var pair in entries)
        {
            if (!string.IsNullOrWhiteSpace(pair.Key) && pair.Value > 0)
            {
                _entries[pair.Key] = pair.Value;
            }
        }
    }

    public IReadOnlyDictionary<string, long> Entries => _entries;

    public VectorClock Increment(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        var next = new Dictionary<string, long>(_entries, StringComparer.Ordinal);
        next[nodeId] = next.TryGetValue(nodeId, out var value) ? value + 1 : 1;
        return new VectorClock(next);
    }

    public VectorClock Merge(VectorClock other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var merged = new Dictionary<string, long>(_entries, StringComparer.Ordinal);
        foreach (var pair in other._entries)
        {
            if (!merged.TryGetValue(pair.Key, out var existing) || pair.Value > existing)
            {
                merged[pair.Key] = pair.Value;
            }
        }

        return new VectorClock(merged);
    }

    public VectorClockComparison Compare(VectorClock other)
    {
        ArgumentNullException.ThrowIfNull(other);

        var keys = _entries.Keys.Concat(other._entries.Keys).Distinct(StringComparer.Ordinal);
        var less = false;
        var greater = false;

        foreach (var key in keys)
        {
            var left = _entries.TryGetValue(key, out var l) ? l : 0;
            var right = other._entries.TryGetValue(key, out var r) ? r : 0;
            if (left < right)
            {
                less = true;
            }
            else if (left > right)
            {
                greater = true;
            }

            if (less && greater)
            {
                return VectorClockComparison.Concurrent;
            }
        }

        if (!less && !greater)
        {
            return VectorClockComparison.Equal;
        }

        return less ? VectorClockComparison.Before : VectorClockComparison.After;
    }
}

public enum VectorClockComparison
{
    Before,
    After,
    Equal,
    Concurrent
}
