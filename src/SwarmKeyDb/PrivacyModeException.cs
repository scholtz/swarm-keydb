namespace SwarmKeyDb;

public sealed class PrivacyModeException : InvalidOperationException
{
    public PrivacyModeException(string message)
        : base(message)
    {
    }
}
