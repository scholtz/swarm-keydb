namespace SwarmKeyDb;

public sealed class SwarmKeyDbOptions
{
    public PrivacyMode PrivacyMode { get; set; } = PrivacyMode.ObliviousHashing;
    public string? PrivacyKeyHex { get; set; }
}
