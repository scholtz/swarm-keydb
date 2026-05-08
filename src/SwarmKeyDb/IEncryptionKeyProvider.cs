namespace SwarmKeyDb;

public interface IEncryptionKeyProvider
{
    EncryptionOptions GetOptions();
    byte[]? GetCurrentKey();
    void Update(EncryptionOptions options);
}
