namespace SwarmKeyDb;

public interface ICrossChainStateStore
{
    Task UpsertAsync(CrossChainSyncRecord record, CancellationToken cancellationToken = default);
    Task<CrossChainSyncRecord?> GetAsync(string key, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<CrossChainSyncRecord>> ListAsync(CancellationToken cancellationToken = default);
}
