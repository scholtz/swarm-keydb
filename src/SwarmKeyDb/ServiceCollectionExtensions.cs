using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
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
            store = new CrdtKeyValueStore(store);
            store = new CachingKeyValueStore(
                store,
                sp.GetRequiredService<IMemoryCache>(),
                sp.GetRequiredService<IOptions<CacheOptions>>(),
                sp.GetRequiredService<ILogger<CachingKeyValueStore>>());
            return store;
        });

        services.AddSingleton<ICacheStats>(sp => (ICacheStats)sp.GetRequiredService<IKeyValueStore>());
        return services;
    }
}
