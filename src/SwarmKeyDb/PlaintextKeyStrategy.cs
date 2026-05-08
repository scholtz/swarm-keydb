namespace SwarmKeyDb;

public sealed class PlaintextKeyStrategy : IKeyPrivacyStrategy
{
    public PrivacyMode Mode => PrivacyMode.None;

    public string DeriveToken(string key) => key;
}
