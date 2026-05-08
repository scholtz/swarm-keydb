namespace SwarmKeyDb;

public interface IAsyncProcessingStore
{
    Task FlushAsync(CancellationToken cancellationToken = default);
    void FireAndForget(Func<Task> operation, string operationName = "fire-and-forget");
    void FireAndForget(Action operation, string operationName = "fire-and-forget");
}
