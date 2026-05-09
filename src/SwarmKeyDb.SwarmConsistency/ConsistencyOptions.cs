namespace SwarmKeyDb.SwarmConsistency;

public sealed class ConsistencyOptions
{
    public bool Enabled { get; set; } = true;
    public ConsistencyFailureMode FailureMode { get; set; } = ConsistencyFailureMode.Strict;
    public int QuorumThreshold { get; set; } = 1;
    public string FeedOwner { get; set; } = "0000000000000000000000000000000000000000";
    public Func<string, ulong?>? ExpectedFeedRevisionResolver { get; set; }
    public Func<string, (string ManifestRef, IReadOnlyList<string> Ancestors)?>? ExpectedManifestLineageResolver { get; set; }
}

public enum ConsistencyFailureMode
{
    Strict = 0,
    Warn = 1
}
