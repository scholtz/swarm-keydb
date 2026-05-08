namespace SwarmKeyDb;

/// <summary>
/// Per-key conflict resolution options.
/// </summary>
public sealed class KeyOptions
{
    /// <summary>
    /// Merge strategy used when writing or merging this key.
    /// Defaults to last-write-wins when null.
    /// </summary>
    public IMergeStrategy? MergeStrategy { get; init; }
}
