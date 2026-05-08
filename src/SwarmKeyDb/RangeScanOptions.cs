namespace SwarmKeyDb;

/// <summary>
/// Controls key-range scanning behavior.
/// </summary>
public sealed class RangeScanOptions
{
    /// <summary>
    /// Includes the <c>startKey</c> when it exists in the index.
    /// </summary>
    public bool IncludeStart { get; init; } = true;

    /// <summary>
    /// Includes the <c>endKey</c> when it exists in the index.
    /// </summary>
    public bool IncludeEnd { get; init; } = true;

    /// <summary>
    /// Returns keys in descending lexicographic order when true.
    /// </summary>
    public bool Descending { get; init; }

    /// <summary>
    /// Includes values in the result set when true.
    /// </summary>
    public bool IncludeValues { get; init; }

    /// <summary>
    /// Optional upper bound on the number of entries to return.
    /// </summary>
    public int? Limit { get; init; }
}
