namespace SwarmKeyDb;

public interface IKeyPrivacyStrategy
{
    PrivacyMode Mode { get; }
    string DeriveToken(string key);
}
