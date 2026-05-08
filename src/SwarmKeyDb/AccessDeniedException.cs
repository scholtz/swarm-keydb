namespace SwarmKeyDb;

public sealed class AccessDeniedException : Exception
{
    public AccessDeniedException(string message)
        : base(message)
    {
    }

    public int StatusCode => 403;
}
