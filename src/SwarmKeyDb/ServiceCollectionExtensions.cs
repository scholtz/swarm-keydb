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
        ArgumentNullException.ThrowIfNull(swarmClient);
        ArgumentNullException.ThrowIfNull(index);
        return services.AddSwarmKeyDbStore(
            sp => new SwarmKeyValueStore(
                swarmClient,
                index,
                sp.GetService<IOptions<IntegrityOptions>>()?.Value));
    }

    public static IServiceCollection AddSwarmKeyDbStore(
        this IServiceCollection services,
        Func<IServiceProvider, IKeyValueStore> baseStoreFactory)
    {
        ArgumentNullException.ThrowIfNull(baseStoreFactory);
        services.AddSingleton<IKeyValueStore>(sp =>
        {
            IKeyValueStore store = baseStoreFactory(sp);
            var keyProvider = sp.GetService<IEncryptionKeyProvider>()
                ?? new MutableEncryptionKeyProvider(sp.GetRequiredService<IOptions<EncryptionOptions>>().Value);

            if (sp.GetRequiredService<IOptions<AclOptions>>().Value.Enabled)
            {
                store = new AclKeyValueStore(
                    store,
                    sp.GetRequiredService<IOptions<AclOptions>>(),
                    sp.GetRequiredService<IEthAddressAccessor>());
            }

            var didOptions = sp.GetService<IOptions<SwarmKeyDbOptions>>()?.Value;
            if (didOptions?.DidMode != DidAuthMode.None)
            {
                var provider = sp.GetService<IDecentralizedIdentityProvider>()
                    ?? new EthrDidProvider(new EthrDidProviderOptions { RpcUrl = didOptions?.DidRpcUrl });
                var contextAccessor = sp.GetService<IDidContextAccessor>()
                    ?? new AsyncLocalDidContextAccessor();
                store = new DidAuthKeyValueStore(store, provider, contextAccessor, didOptions!.DidMode);
            }

            store = new EncryptingKeyValueStore(
                store,
                keyProvider,
                sp.GetRequiredService<ILogger<EncryptingKeyValueStore>>());

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

            var swarmOptions = sp.GetService<IOptions<SwarmKeyDbOptions>>()?.Value ?? new SwarmKeyDbOptions();
            if (swarmOptions.OfflineMode != OfflineMode.Never)
            {
                IOfflineJournal journal = swarmOptions.OfflineJournal == OfflineJournalType.Sqlite
                    ? new SqliteOfflineJournal(
                        swarmOptions.OfflineSqlitePath
                        ?? Path.Combine(AppContext.BaseDirectory, "data", "offline-journal.sqlite"))
                    : new InMemoryOfflineJournal();
                var connectivityProbe = sp.GetService<IConnectivityProbe>() ?? new AlwaysConnectedProbe();
                store = new OfflineCapableKeyValueStore(
                    store,
                    journal,
                    connectivityProbe,
                    swarmOptions,
                    sp.GetService<ILogger<OfflineCapableKeyValueStore>>() ?? NullLogger<OfflineCapableKeyValueStore>.Instance);
            }

            var asyncOptions = sp.GetService<IOptions<AsyncProcessingOptions>>()?.Value ?? new AsyncProcessingOptions();
            if (asyncOptions.Enabled && swarmOptions.OfflineMode == OfflineMode.Never)
            {
                store = new AsyncQueuedKeyValueStore(
                    store,
                    asyncOptions,
                    sp.GetService<ILogger<AsyncQueuedKeyValueStore>>() ?? NullLogger<AsyncQueuedKeyValueStore>.Instance);
            }

            return store;
        });

        services.AddSingleton<ICacheStats>(sp => (ICacheStats)sp.GetRequiredService<IKeyValueStore>());
        services.AddSingleton<IOfflineStatusProvider>(sp =>
            sp.GetRequiredService<IKeyValueStore>() as IOfflineStatusProvider ?? NoOpOfflineStatusProvider.Instance);
        return services;
    }
}
