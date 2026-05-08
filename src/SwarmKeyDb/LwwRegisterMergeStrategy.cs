namespace SwarmKeyDb;

/// <summary>
/// Last-write-wins register strategy with vector-clock ordering and deterministic tie-breaking.
/// </summary>
public sealed class LwwRegisterMergeStrategy : IMergeStrategy
{
    public static LwwRegisterMergeStrategy Instance { get; } = new();

    public string Name => "lww-register";

    public CrdtValue Merge(string key, CrdtValue? existing, CrdtValue incoming)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(incoming);

        if (existing is null)
        {
            return incoming;
        }

        var ordering = existing.VectorClock.Compare(incoming.VectorClock);
        var winner = ordering switch
        {
            VectorClockComparison.Before => incoming,
            VectorClockComparison.After => existing,
            _ => PickByTimestampThenNode(existing, incoming)
        };

        return winner with { VectorClock = existing.VectorClock.Merge(incoming.VectorClock) };
    }

    private static CrdtValue PickByTimestampThenNode(CrdtValue left, CrdtValue right)
    {
        if (left.TimestampUtc != right.TimestampUtc)
        {
            return left.TimestampUtc > right.TimestampUtc ? left : right;
        }

        // Deterministic tie-break: larger node id wins; identical ids keep the left value.
        return string.CompareOrdinal(left.WriterNodeId, right.WriterNodeId) >= 0 ? left : right;
    }
}
