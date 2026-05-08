namespace SwarmKeyDb;

public sealed class NamespacedChainAdapter : IChainAdapter
{
    private readonly IKeyValueStore _store;
    private readonly string _prefix;

    public NamespacedChainAdapter(IKeyValueStore store, ChainAdapterOptions options)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(options);
        _store = store;
        ChainId = options.ChainId;
        Name = string.IsNullOrWhiteSpace(options.Name) ? $"chain-{options.ChainId}" : options.Name;
        RpcUrl = options.RpcUrl;
        BridgeContractAddress = options.BridgeContractAddress;
        _prefix = $"chain:{options.ChainId}:";
    }

    public int ChainId { get; }

    public string Name { get; }

    public string? RpcUrl { get; }

    public string? BridgeContractAddress { get; }

    public string GetNamespacedKey(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return _prefix + key;
    }

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(GetNamespacedKey(key), value, cancellationToken);

    public Task DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _store.DeleteAsync(GetNamespacedKey(key), cancellationToken);
}
