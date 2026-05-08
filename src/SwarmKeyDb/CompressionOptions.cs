namespace SwarmKeyDb;

public sealed class CompressionOptions
{
    public bool Enabled { get; set; } = false;
    public CompressionAlgorithm Algorithm { get; set; } = CompressionAlgorithm.GZip;
    public int MinSizeBytes { get; set; } = 64;
}
