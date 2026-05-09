namespace SwarmKeyDb;

public interface IInMemoryCacheSyncBusControl
{
    void SetNodeConnected(string nodeId, bool connected);
}

public sealed class InMemoryCacheSyncBus : ICacheSyncBusWithNodeSubscriptions, ICacheSyncPeerStateBus, IInMemoryCacheSyncBusControl
{
    private readonly object _gate = new();
    private readonly Dictionary<Guid, Subscription> _subscriptions = new();
    private readonly Dictionary<string, Func<CancellationToken, Task<IReadOnlyDictionary<string, long>>>> _versionProviders = new(StringComparer.Ordinal);
    private readonly Dictionary<string, bool> _nodeConnectivity = new(StringComparer.Ordinal);

    public Task PublishInvalidationAsync(CacheInvalidationEvent invalidation, CancellationToken cancellationToken = default)
    {
        Subscription[] subscriptions;
        lock (_gate)
        {
            subscriptions = _subscriptions.Values.ToArray();
        }

        return FanOutAsync(subscriptions, invalidation, cancellationToken);
    }

    public IDisposable SubscribeInvalidations(Func<CacheInvalidationEvent, Task> handler) =>
        SubscribeInvalidations(nodeId: string.Empty, handler);

    public IDisposable SubscribeInvalidations(string nodeId, Func<CacheInvalidationEvent, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var id = Guid.NewGuid();
        lock (_gate)
        {
            _subscriptions[id] = new Subscription(nodeId, handler);
            if (!string.IsNullOrWhiteSpace(nodeId) && !_nodeConnectivity.ContainsKey(nodeId))
            {
                _nodeConnectivity[nodeId] = true;
            }
        }

        return new CallbackDisposable(() =>
        {
            lock (_gate)
            {
                _subscriptions.Remove(id);
            }
        });
    }

    public IDisposable RegisterVersionProvider(string nodeId, Func<CancellationToken, Task<IReadOnlyDictionary<string, long>>> provider)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(provider);
        lock (_gate)
        {
            _versionProviders[nodeId] = provider;
            _nodeConnectivity[nodeId] = true;
        }

        return new CallbackDisposable(() =>
        {
            lock (_gate)
            {
                _versionProviders.Remove(nodeId);
                _nodeConnectivity.Remove(nodeId);
            }
        });
    }

    public async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, long>>> GetPeerVersionStampsAsync(string requesterNodeId, CancellationToken cancellationToken = default)
    {
        KeyValuePair<string, Func<CancellationToken, Task<IReadOnlyDictionary<string, long>>>>[] peers;
        lock (_gate)
        {
            peers = _versionProviders
                .Where(static pair => !string.IsNullOrWhiteSpace(pair.Key))
                .Where(pair => !string.Equals(pair.Key, requesterNodeId, StringComparison.Ordinal))
                .Where(pair => IsNodeConnectedUnsafe(pair.Key))
                .ToArray();
        }

        var result = new Dictionary<string, IReadOnlyDictionary<string, long>>(StringComparer.Ordinal);
        foreach (var peer in peers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            result[peer.Key] = await peer.Value(cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    public int GetConnectedPeerCount(string requesterNodeId)
    {
        lock (_gate)
        {
            return _versionProviders.Keys
                .Where(static key => !string.IsNullOrWhiteSpace(key))
                .Where(key => !string.Equals(key, requesterNodeId, StringComparison.Ordinal))
                .Count(IsNodeConnectedUnsafe);
        }
    }

    public void SetNodeConnected(string nodeId, bool connected)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        lock (_gate)
        {
            _nodeConnectivity[nodeId] = connected;
        }
    }

    private async Task FanOutAsync(IReadOnlyList<Subscription> subscriptions, CacheInvalidationEvent invalidation, CancellationToken cancellationToken)
    {
        foreach (var subscription in subscriptions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!string.IsNullOrWhiteSpace(subscription.NodeId))
            {
                lock (_gate)
                {
                    if (!IsNodeConnectedUnsafe(subscription.NodeId))
                    {
                        continue;
                    }
                }
            }

            await subscription.Handler(invalidation).ConfigureAwait(false);
        }
    }

    private bool IsNodeConnectedUnsafe(string nodeId) =>
        !_nodeConnectivity.TryGetValue(nodeId, out var connected) || connected;

    private sealed record Subscription(string NodeId, Func<CacheInvalidationEvent, Task> Handler);

    private sealed class CallbackDisposable : IDisposable
    {
        private readonly Action _callback;
        private int _disposed;

        public CallbackDisposable(Action callback)
        {
            _callback = callback;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _callback();
        }
    }
}
