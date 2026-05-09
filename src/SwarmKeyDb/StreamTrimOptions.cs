namespace SwarmKeyDb;

public sealed class StreamTrimOptions
{
    public long? DefaultMaxLen { get; set; }
    public bool DefaultMaxLenApproximate { get; set; } = true;
}
