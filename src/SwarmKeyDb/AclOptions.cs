namespace SwarmKeyDb;

public sealed class AclOptions
{
    public bool Enabled { get; set; } = false;
    public AclMode Mode { get; set; } = AclMode.Allowlist;
    public List<AclEntry> Entries { get; set; } = [];
}
