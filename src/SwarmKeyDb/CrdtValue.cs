namespace SwarmKeyDb;

/// <summary>
/// Value + CRDT metadata persisted for conflict-free merge resolution.
/// </summary>
public sealed record CrdtValue(
    byte[] Value,
    VectorClock VectorClock,
    DateTimeOffset TimestampUtc,
    string WriterNodeId);
