namespace SwarmKeyDb;

/// <summary>
/// Merge strategy for PN-counter payloads serialized with <see cref="PnCounterValue"/>.
/// </summary>
public sealed class PnCounterMergeStrategy : IMergeStrategy
{
    public static PnCounterMergeStrategy Instance { get; } = new();

    public string Name => "pn-counter";

    public CrdtValue Merge(string key, CrdtValue? existing, CrdtValue incoming)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(incoming);

        if (existing is null)
        {
            return incoming;
        }

        var mergedCounter = PnCounterValue.FromByteArray(existing.Value).Merge(PnCounterValue.FromByteArray(incoming.Value));
        return new CrdtValue(
            mergedCounter.ToByteArray(),
            existing.VectorClock.Merge(incoming.VectorClock),
            existing.TimestampUtc >= incoming.TimestampUtc ? existing.TimestampUtc : incoming.TimestampUtc,
            ChooseDeterministicWriter(existing.WriterNodeId, incoming.WriterNodeId));
    }

    private static string ChooseDeterministicWriter(string left, string right) =>
        string.CompareOrdinal(left, right) >= 0 ? left : right;
}
