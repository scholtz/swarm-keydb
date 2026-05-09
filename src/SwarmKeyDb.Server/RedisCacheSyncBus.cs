using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace SwarmKeyDb.Server;

public sealed class RedisCacheSyncBus : ICacheSyncBus, IDisposable
{
    private readonly ConnectionMultiplexer _multiplexer;
    private readonly ISubscriber _subscriber;
    private readonly RedisChannel _channel;
    private readonly ILogger<RedisCacheSyncBus> _logger;
    private readonly object _gate = new();
    private readonly Dictionary<Guid, Func<CacheInvalidationEvent, Task>> _handlers = new();
    private bool _subscribed;

    public RedisCacheSyncBus(IReadOnlyList<string> peers, string channel, ILogger<RedisCacheSyncBus> logger)
    {
        _logger = logger;
        var endpoints = peers?.Where(static value => !string.IsNullOrWhiteSpace(value)).ToArray() ?? [];
        if (endpoints.Length == 0)
        {
            throw new InvalidOperationException("Redis cache sync bus requires at least one peer endpoint.");
        }

        _multiplexer = ConnectionMultiplexer.Connect(string.Join(',', endpoints));
        _subscriber = _multiplexer.GetSubscriber();
        _channel = new RedisChannel(string.IsNullOrWhiteSpace(channel) ? "swarm-keydb-sync" : channel, RedisChannel.PatternMode.Literal);
    }

    public async Task PublishInvalidationAsync(CacheInvalidationEvent invalidation, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var payload = JsonSerializer.Serialize(invalidation);
        _ = await _subscriber.PublishAsync(_channel, payload).ConfigureAwait(false);
    }

    public IDisposable SubscribeInvalidations(Func<CacheInvalidationEvent, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        EnsureSubscribed();
        var id = Guid.NewGuid();
        lock (_gate)
        {
            _handlers[id] = handler;
        }

        return new Subscription(() =>
        {
            lock (_gate)
            {
                _handlers.Remove(id);
            }
        });
    }

    private void EnsureSubscribed()
    {
        lock (_gate)
        {
            if (_subscribed)
            {
                return;
            }
        }

        _subscriber.Subscribe(_channel, (_, value) =>
        {
            CacheInvalidationEvent? invalidation;
            try
            {
                invalidation = JsonSerializer.Deserialize<CacheInvalidationEvent>(value.ToString());
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize cache invalidation message from Redis sync channel.");
                return;
            }

            if (invalidation is null)
            {
                return;
            }

            Func<CacheInvalidationEvent, Task>[] handlers;
            lock (_gate)
            {
                handlers = _handlers.Values.ToArray();
            }

            foreach (var handler in handlers)
            {
                try
                {
                    handler(invalidation);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Cache invalidation handler threw while processing Redis sync message.");
                }
            }
        });

        lock (_gate)
        {
            _subscribed = true;
        }
    }

    public void Dispose()
    {
        _multiplexer.Dispose();
    }

    private sealed class Subscription : IDisposable
    {
        private readonly Action _onDispose;
        private int _disposed;

        public Subscription(Action onDispose)
        {
            _onDispose = onDispose;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _onDispose();
        }
    }
}
