namespace SwarmKeyDb;

public interface IAccessControlVerifier
{
    void EnsureReadAccess();
    void EnsureWriteAccess();
}
