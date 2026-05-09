namespace SwarmKeyDb.SwarmConsistency;

public sealed class ConsistencyOptions
{
    public bool Enabled { get; set; } = true;
    public ConsistencyFailureMode FailureMode { get; set; } = ConsistencyFailureMode.Strict;
    public int QuorumThreshold { get; set; } = 1;
    public string FeedOwner { get; set; } = "0000000000000000000000000000000000000000";
    public Func<string, ulong?>? ExpectedFeedRevisionResolver { get; set; }
    public Func<string, (string ManifestRef, IReadOnlyList<string> Ancestors)?>? ExpectedManifestLineageResolver { get; set; }

    /// <summary>
    /// Optional callback invoked when a consistency verification fails (in
    /// <see cref="ConsistencyFailureMode.Warn"/> mode).  Receives the affected key and the
    /// <see cref="VerificationResult"/> that caused the failure.  The default behaviour is
    /// to log a warning and evict the key from the cache; this callback runs in addition
    /// to (not instead of) the built-in behaviour.
    /// </summary>
    public Action<string, VerificationResult>? OnVerificationFailure { get; set; }
}

public enum ConsistencyFailureMode
{
    Strict = 0,
    Warn = 1
}
