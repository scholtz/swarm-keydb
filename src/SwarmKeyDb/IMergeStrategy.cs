namespace SwarmKeyDb;

/// <summary>
/// Resolves conflicting values for the same key into a deterministic merged value.
/// </summary>
public interface IMergeStrategy
{
    string Name { get; }

    CrdtValue Merge(string key, CrdtValue? existing, CrdtValue incoming);
}
