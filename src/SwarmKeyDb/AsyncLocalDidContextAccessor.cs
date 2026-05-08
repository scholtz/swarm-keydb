namespace SwarmKeyDb;

/// <summary>
/// <see cref="AsyncLocal{T}"/>-backed implementation of <see cref="IDidContextAccessor"/>.
/// Each asynchronous call chain (e.g. a Redis connection handler) gets its own isolated context.
/// </summary>
public sealed class AsyncLocalDidContextAccessor : IDidContextAccessor
{
    private readonly AsyncLocal<DidContext?> _current = new();

    /// <inheritdoc/>
    public DidContext? Current
    {
        get => _current.Value;
        set => _current.Value = value;
    }
}
