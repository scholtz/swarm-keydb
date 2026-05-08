namespace SwarmKeyDb;

/// <summary>
/// A page returned by cursor-based key iteration.
/// </summary>
/// <param name="NextCursor">
/// Opaque cursor to resume iteration, or empty when iteration has reached the end.
/// </param>
/// <param name="Keys">Keys in the current page.</param>
public readonly record struct ScanResult(string NextCursor, IReadOnlyList<string> Keys);
