using System.Text.Json;

namespace SwarmKeyDb;

/// <summary>
/// Observed-remove set state for add/remove/merge semantics.
/// </summary>
public sealed class OrSetValue
{
    private readonly Dictionary<string, HashSet<string>> _adds;
    private readonly HashSet<string> _removed;

    public static OrSetValue Empty { get; } = new(
        new Dictionary<string, IEnumerable<string>>(StringComparer.Ordinal),
        Array.Empty<string>());

    public OrSetValue(
        IReadOnlyDictionary<string, IEnumerable<string>> adds,
        IEnumerable<string> removed)
    {
        _adds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (var pair in adds)
        {
            _adds[pair.Key] = new HashSet<string>(pair.Value.Where(static tag => !string.IsNullOrWhiteSpace(tag)), StringComparer.Ordinal);
        }

        _removed = new HashSet<string>(removed.Where(static tag => !string.IsNullOrWhiteSpace(tag)), StringComparer.Ordinal);
    }

    public IReadOnlyList<string> Elements => _adds
        .Where(pair => pair.Value.Any(tag => !_removed.Contains(tag)))
        .Select(pair => pair.Key)
        .Order(StringComparer.Ordinal)
        .ToArray();

    public OrSetValue Add(string element, string tag)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(element);
        ArgumentException.ThrowIfNullOrWhiteSpace(tag);

        var adds = CloneAdds();
        if (!adds.TryGetValue(element, out var tags))
        {
            tags = new HashSet<string>(StringComparer.Ordinal);
            adds[element] = tags;
        }

        tags.Add(tag);
        return new OrSetValue(ToEnumerableMap(adds), _removed);
    }

    public OrSetValue Remove(string element)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(element);
        var removed = new HashSet<string>(_removed, StringComparer.Ordinal);
        if (_adds.TryGetValue(element, out var tags))
        {
            foreach (var tag in tags)
            {
                removed.Add(tag);
            }
        }

        return new OrSetValue(ToEnumerableMap(CloneAdds()), removed);
    }

    public OrSetValue Merge(OrSetValue other)
    {
        ArgumentNullException.ThrowIfNull(other);

        var adds = CloneAdds();
        foreach (var pair in other._adds)
        {
            if (!adds.TryGetValue(pair.Key, out var tags))
            {
                tags = new HashSet<string>(StringComparer.Ordinal);
                adds[pair.Key] = tags;
            }

            tags.UnionWith(pair.Value);
        }

        var removed = new HashSet<string>(_removed, StringComparer.Ordinal);
        removed.UnionWith(other._removed);
        return new OrSetValue(ToEnumerableMap(adds), removed);
    }

    public byte[] ToByteArray()
    {
        var dto = new OrSetDto(
            _adds.ToDictionary(
                static pair => pair.Key,
                static pair => pair.Value.Order(StringComparer.Ordinal).ToArray(),
                StringComparer.Ordinal),
            _removed.Order(StringComparer.Ordinal).ToArray());
        return JsonSerializer.SerializeToUtf8Bytes(dto);
    }

    public static OrSetValue FromByteArray(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
        {
            return Empty;
        }

        var dto = JsonSerializer.Deserialize<OrSetDto>(data);
        return dto is null
            ? Empty
            : new OrSetValue(
                (dto.Adds ?? new Dictionary<string, string[]>(StringComparer.Ordinal))
                    .ToDictionary(
                        static pair => pair.Key,
                        static pair => (IEnumerable<string>)pair.Value,
                        StringComparer.Ordinal),
                dto.Removed ?? Array.Empty<string>());
    }

    private Dictionary<string, HashSet<string>> CloneAdds() =>
        _adds.ToDictionary(
            static pair => pair.Key,
            static pair => new HashSet<string>(pair.Value, StringComparer.Ordinal),
            StringComparer.Ordinal);

    private static IReadOnlyDictionary<string, IEnumerable<string>> ToEnumerableMap(
        Dictionary<string, HashSet<string>> adds) =>
        adds.ToDictionary(
            static pair => pair.Key,
            static pair => (IEnumerable<string>)pair.Value,
            StringComparer.Ordinal);

    private sealed record OrSetDto(
        Dictionary<string, string[]>? Adds,
        string[]? Removed);
}
