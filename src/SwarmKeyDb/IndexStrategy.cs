namespace SwarmKeyDb;

/// <summary>
/// Selects the indexing strategy used for key lookups and range scans.
/// </summary>
public enum IndexStrategy
{
    /// <summary>
    /// Hash-map based index with O(1) single-key lookups.
    /// Range scans require a full enumeration and sort, giving O(n log n) per scan.
    /// This is the legacy default and is appropriate for small key sets.
    /// </summary>
    Dictionary,

    /// <summary>
    /// Sorted Red-Black tree (B-tree style) index backed by <see cref="System.Collections.Generic.SortedDictionary{TKey,TValue}"/>.
    /// Provides O(log n) single-key lookups and O(k) range / prefix scans where k is the result count.
    /// Recommended for key sets larger than a few hundred entries.
    /// </summary>
    BTree,
}
