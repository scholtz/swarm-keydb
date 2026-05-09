namespace SwarmKeyDb;

/// <summary>
/// Thrown when a store operation is denied because the caller's DID could not be
/// authenticated or does not have permission for the requested operation.
/// </summary>
public sealed class DidAuthorizationException : Exception
{
    /// <param name="message">Human-readable description of the authorization failure.</param>
    public DidAuthorizationException(string message)
        : base(message)
    {
    }

    /// <param name="message">Human-readable description of the authorization failure.</param>
    /// <param name="innerException">The inner exception that caused this failure.</param>
    public DidAuthorizationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>HTTP status code that should be returned for this exception (403 Forbidden).</summary>
    public int StatusCode => 403;
}
