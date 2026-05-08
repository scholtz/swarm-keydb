namespace SwarmKeyDb;

/// <summary>
/// A single entry returned from a key range scan.
/// </summary>
/// <param name="Key">The key name.</param>
/// <param name="Value">Optional key value. Present only when requested by scan options.</param>
public readonly record struct RangeScanEntry(string Key, byte[]? Value);
