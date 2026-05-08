using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SwarmKeyDb;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSwarmKeyDbStore(this IServiceCollection services, ISwarmClient swarmClient, IKeyIndex index)
    {
        services.AddSingleton<IKeyValueStore>(sp =>
        {
            IKeyValueStore store = new SwarmKeyValueStore(swarmClient, index);

            if (sp.GetRequiredService<IOptions<AclOptions>>().Value.Enabled)
            {
                store = new AclKeyValueStore(
                    store,
                    sp.GetRequiredService<IOptions<AclOptions>>(),
                    sp.GetRequiredService<IEthAddressAccessor>());
            }

            if (sp.GetRequiredService<IOptions<EncryptionOptions>>().Value.Enabled)
            {
                store = new EncryptingKeyValueStore(
                    store,
                    sp.GetRequiredService<IOptions<EncryptionOptions>>(),
                    sp.GetRequiredService<ILogger<EncryptingKeyValueStore>>());
            }

            store = new CompressingKeyValueStore(
                store,
                sp.GetRequiredService<IOptions<CompressionOptions>>(),
                sp.GetRequiredService<ILogger<CompressingKeyValueStore>>());
            store = new CrdtKeyValueStore(store, Environment.GetEnvironmentVariable("SWARM_KEYDB_CRDT_NODE_ID"));
            store = new CachingKeyValueStore(
                store,
                sp.GetRequiredService<IMemoryCache>(),
                sp.GetRequiredService<IOptions<CacheOptions>>(),
                sp.GetRequiredService<ILogger<CachingKeyValueStore>>());

            var asyncOptions = sp.GetService<IOptions<AsyncProcessingOptions>>()?.Value ?? new AsyncProcessingOptions();
            if (asyncOptions.Enabled)
            {
                store = new AsyncQueuedKeyValueStore(
                    store,
                    asyncOptions,
                    sp.GetService<ILogger<AsyncQueuedKeyValueStore>>() ?? NullLogger<AsyncQueuedKeyValueStore>.Instance);
            }

            return store;
        });

        services.AddSingleton<ICacheStats>(sp => ResolveCacheStats(sp.GetRequiredService<IKeyValueStore>()));
        return services;
    }

    private static ICacheStats ResolveCacheStats(IKeyValueStore store)
    {
        if (store is ICacheStats stats)
        {
            return stats;
        }

        object? current = store;
        while (current is not null)
        {
            var innerField = current.GetType().GetField("_inner", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            if (innerField is null)
            {
                break;
            }

            current = innerField.GetValue(current);
            if (current is ICacheStats cacheStats)
            {
                return cacheStats;
            }
        }

        throw new InvalidOperationException("Unable to resolve cache statistics provider from the configured key-value store pipeline.");
    }
}
