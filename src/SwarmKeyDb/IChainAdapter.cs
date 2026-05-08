namespace SwarmKeyDb;

public interface IChainAdapter
{
    int ChainId { get; }
    string Name { get; }
    string? RpcUrl { get; }
    string? BridgeContractAddress { get; }
    string GetNamespacedKey(string key);
    Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default);
    Task DeleteAsync(string key, CancellationToken cancellationToken = default);
}
