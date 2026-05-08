using System.Text.Json;

namespace SwarmKeyDb;

/// <summary>
/// Positive/negative counter CRDT for convergent increments and decrements.
/// </summary>
public sealed class PnCounterValue
{
    private readonly Dictionary<string, long> _positive;
    private readonly Dictionary<string, long> _negative;

    public static PnCounterValue Zero { get; } = new(
        new Dictionary<string, long>(StringComparer.Ordinal),
        new Dictionary<string, long>(StringComparer.Ordinal));

    public PnCounterValue(IReadOnlyDictionary<string, long> positive, IReadOnlyDictionary<string, long> negative)
    {
        _positive = positive.Where(static pair => pair.Value > 0)
            .ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);
        _negative = negative.Where(static pair => pair.Value > 0)
            .ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);
    }

    public long Value => _positive.Values.Sum() - _negative.Values.Sum();

    public PnCounterValue Increment(string nodeId, long amount = 1)
    {
        if (amount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(amount), "Increment amount must be greater than zero.");
        }

        return Update(nodeId, amount, increase: true);
    }

    public PnCounterValue Decrement(string nodeId, long amount = 1)
    {
        if (amount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(amount), "Decrement amount must be greater than zero.");
        }

        return Update(nodeId, amount, increase: false);
    }

    public PnCounterValue Merge(PnCounterValue other)
    {
        ArgumentNullException.ThrowIfNull(other);

        return new PnCounterValue(
            MergeMaps(_positive, other._positive),
            MergeMaps(_negative, other._negative));
    }

    public byte[] ToByteArray() => JsonSerializer.SerializeToUtf8Bytes(new PnCounterDto(_positive, _negative));

    public static PnCounterValue FromByteArray(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
        {
            return Zero;
        }

        var dto = JsonSerializer.Deserialize<PnCounterDto>(data);
        return dto is null
            ? Zero
            : new PnCounterValue(dto.Positive ?? new Dictionary<string, long>(StringComparer.Ordinal), dto.Negative ?? new Dictionary<string, long>(StringComparer.Ordinal));
    }

    private PnCounterValue Update(string nodeId, long amount, bool increase)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        var positive = new Dictionary<string, long>(_positive, StringComparer.Ordinal);
        var negative = new Dictionary<string, long>(_negative, StringComparer.Ordinal);

        var map = increase ? positive : negative;
        map[nodeId] = map.TryGetValue(nodeId, out var current) ? current + amount : amount;

        return new PnCounterValue(positive, negative);
    }

    private static Dictionary<string, long> MergeMaps(
        IReadOnlyDictionary<string, long> left,
        IReadOnlyDictionary<string, long> right)
    {
        var merged = new Dictionary<string, long>(left, StringComparer.Ordinal);
        foreach (var pair in right)
        {
            if (!merged.TryGetValue(pair.Key, out var existing) || pair.Value > existing)
            {
                merged[pair.Key] = pair.Value;
            }
        }

        return merged;
    }

    private sealed record PnCounterDto(
        Dictionary<string, long>? Positive,
        Dictionary<string, long>? Negative);
}
