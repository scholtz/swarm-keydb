namespace SwarmKeyDb;

public interface IRedisCommandObserver
{
    void OnCommandCompleted(
        string command,
        string operation,
        bool succeeded,
        string? errorType,
        TimeSpan elapsed,
        string correlationId);
}
