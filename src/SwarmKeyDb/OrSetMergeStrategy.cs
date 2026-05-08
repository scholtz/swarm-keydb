namespace SwarmKeyDb;

/// <summary>
/// Merge strategy for OR-Set payloads serialized with <see cref="OrSetValue"/>.
/// </summary>
public sealed class OrSetMergeStrategy : IMergeStrategy
{
    public static OrSetMergeStrategy Instance { get; } = new();

    public string Name => "or-set";

    public CrdtValue Merge(string key, CrdtValue? existing, CrdtValue incoming)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(incoming);

        if (existing is null)
        {
            return incoming;
        }

        var mergedSet = OrSetValue.FromByteArray(existing.Value).Merge(OrSetValue.FromByteArray(incoming.Value));
        return new CrdtValue(
            mergedSet.ToByteArray(),
            existing.VectorClock.Merge(incoming.VectorClock),
            existing.TimestampUtc >= incoming.TimestampUtc ? existing.TimestampUtc : incoming.TimestampUtc,
            ChooseDeterministicWriter(existing.WriterNodeId, incoming.WriterNodeId));
    }

    private static string ChooseDeterministicWriter(string left, string right) =>
        string.CompareOrdinal(left, right) >= 0 ? left : right;
}
