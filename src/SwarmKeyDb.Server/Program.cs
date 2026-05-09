using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;
using SwarmKeyDb;
using SwarmKeyDb.SwarmConsistency;
using SwarmKeyDb.Server;

var appSettings = LoadAppSettings();
var port = GetInt("SWARM_KEYDB_PORT", 6379);
var bind = IPAddress.Parse(GetString("SWARM_KEYDB_BIND", "0.0.0.0"));
var dataDir = GetString("SWARM_KEYDB_DATA_DIR", Path.Combine(AppContext.BaseDirectory, "data"));
var backend = (GetFirstSetting("BACKEND", "SWARM_KEYDB_BACKEND") ?? "local").ToLowerInvariant();
var ipfsApiUrl = GetFirstSetting("IPFS_API_URL", "Ipfs:ApiUrl") ?? "http://localhost:5001/";
var ipfsPinOnWrite = GetBoolFromMany(defaultValue: true, "IPFS_PIN_ON_WRITE", "Ipfs:PinOnWrite");
var metricsEnabled = GetBool("METRICS_ENABLED", true);
var metricsPort = GetInt("METRICS_PORT", 9090);
var dashboardEnabled = GetBool("DASHBOARD_ENABLED", true);
var dashboardPort = GetInt("DASHBOARD_PORT", 8080);
var privacyMode = GetPrivacyModeFromSettings();
var privacyKeyHex = GetFirstSetting("SWARM_KEYDB_PRIVACY_KEY", "SWARM_KEYDB_ENCRYPTION_ETH_KEY", "Privacy:KeyHex");
var didMode = GetDidModeFromSettings();
var didRpcUrl = GetFirstSetting("SWARM_KEYDB_DID_RPC_URL", "Did:RpcUrl");
var didMethod = GetFirstSetting("SWARM_KEYDB_DID_METHOD", "Did:Method") ?? "ethr";
var offlineMode = GetOfflineModeFromSettings();
var offlineJournal = GetOfflineJournalFromSettings();
var offlineSyncIntervalMs = Math.Max(250, GetInt("SWARM_KEYDB_OFFLINE_SYNC_INTERVAL_MS", 5_000));
var privacyOptions = new SwarmKeyDbOptions
{
    PrivacyMode = privacyMode,
    PrivacyKeyHex = privacyKeyHex,
    OfflineMode = offlineMode,
    OfflineJournal = offlineJournal,
    OfflineSyncIntervalMs = offlineSyncIntervalMs,
    OfflineSqlitePath = Path.Combine(dataDir, "offline-journal.sqlite"),
    DidMode = didMode,
    DidRpcUrl = didRpcUrl,
    DidMethod = didMethod
};

var logLevel = GetLogLevel("LOG_LEVEL", GetLogLevel("SWARM_KEYDB_LOG_LEVEL", LogLevel.Information));
var environment = GetString("DOTNET_ENVIRONMENT", GetString("ASPNETCORE_ENVIRONMENT", "Production"));
var useJsonLogging = GetBool("JSON_LOGS", !environment.Equals("Development", StringComparison.OrdinalIgnoreCase));

ICacheStats? cacheStats = null;
IOfflineStatusProvider? offlineStatusMetrics = null;
IConsistencyVerificationStatusProvider? consistencyStatusMetrics = null;
ICacheSyncStatusProvider? cacheSyncStatusMetrics = null;
PubSubManager? pubSubManager = null;
var monitoringMetrics = new MonitoringMetrics(
    () => cacheStats ?? NoOpCacheStats.Instance,
    () => offlineStatusMetrics ?? NoOpOfflineStatusProvider.Instance,
    () => consistencyStatusMetrics ?? NoOpConsistencyVerificationStatusProvider.Instance,
    () => cacheSyncStatusMetrics ?? NoOpCacheSyncStatusProvider.Instance,
    privacyMode: privacyOptions.PrivacyMode,
    pubSubManagerAccessor: () => pubSubManager);

var cacheOptions = new CacheOptions
{
    Enabled = GetBool("SWARM_KEYDB_CACHE_ENABLED", true),
    MaxEntries = Math.Max(CacheOptions.MinimumMaxEntries, GetInt("SWARM_KEYDB_CACHE_MAX_ENTRIES", 1_000)),
    DefaultEntryTtl = GetNullableInt("SWARM_KEYDB_CACHE_DEFAULT_TTL_SECONDS") is { } ttlSeconds
        ? TimeSpan.FromSeconds(ttlSeconds)
        : null
};
var compressionOptions = new CompressionOptions
{
    Enabled = GetBool("SWARM_KEYDB_COMPRESSION_ENABLED", false),
    Algorithm = Enum.TryParse<CompressionAlgorithm>(
        GetSetting("SWARM_KEYDB_COMPRESSION_ALGORITHM"), ignoreCase: true, out var compressionAlgorithm)
        ? compressionAlgorithm
        : CompressionAlgorithm.GZip,
    MinSizeBytes = GetInt("SWARM_KEYDB_COMPRESSION_MIN_SIZE_BYTES", 64)
};
var encryptionOptions = new EncryptionOptions
{
    Enabled = GetBool("SWARM_KEYDB_ENCRYPTION_ENABLED", false),
    Algorithm = Enum.TryParse<EncryptionAlgorithm>(
        GetSetting("SWARM_KEYDB_ENCRYPTION_ALGORITHM"), ignoreCase: true, out var encryptionAlgorithm)
        ? encryptionAlgorithm
        : EncryptionAlgorithm.AesGcm256,
    KeyHex = GetSetting("SWARM_KEYDB_ENCRYPTION_KEY"),
    EthPrivateKeyHex = GetSetting("SWARM_KEYDB_ENCRYPTION_ETH_KEY")
};
var integrityOptions = new IntegrityOptions
{
    Enabled = GetBool("SWARM_KEYDB_INTEGRITY_ENABLED", true)
};
var consistencyEnabled = GetBool("SWARM_KEYDB_CONSISTENCY_ENABLED", false);
var consistencyFailureMode = Enum.TryParse<ConsistencyFailureMode>(
    GetString("SWARM_KEYDB_CONSISTENCY_FAILURE_MODE", "Strict"),
    ignoreCase: true,
    out var parsedConsistencyFailureMode)
    ? parsedConsistencyFailureMode
    : ConsistencyFailureMode.Strict;
var consistencyQuorumThreshold = Math.Max(1, GetInt("SWARM_KEYDB_CONSISTENCY_QUORUM_THRESHOLD", 1));
var consistencyFeedOwner = GetString("SWARM_KEYDB_CONSISTENCY_FEED_OWNER", "0000000000000000000000000000000000000000");
var consistencyBeeNodesRaw = GetSetting("SWARM_KEYDB_CONSISTENCY_BEE_NODES");
var shardingOptions = GetShardingOptions();
shardingOptions.Validate();
var aclOptions = GetAclOptions();
var asyncProcessingOptions = new AsyncProcessingOptions
{
    Enabled = GetBool("SWARM_KEYDB_ASYNC_ENABLED", true),
    MaxConcurrentWrites = Math.Max(1, GetInt("SWARM_KEYDB_MAX_CONCURRENT_WRITES", 4)),
    WriteBatchSize = Math.Max(1, GetInt("SWARM_KEYDB_WRITE_BATCH_SIZE", 64)),
    BatchFlushIntervalMs = Math.Max(0, GetInt("SWARM_KEYDB_BATCH_FLUSH_INTERVAL_MS", 100))
};
var ethereumBridgeOptions = new EthereumBridgeOptions
{
    Enabled = GetBoolFromMany(defaultValue: false, "ETH_BRIDGE_ENABLED", "Ethereum:Enabled"),
    RpcUrl = GetFirstSetting("ETH_RPC_URL", "Ethereum:RpcUrl"),
    ContractAddress = GetFirstSetting("ETH_CONTRACT_ADDRESS", "Ethereum:ContractAddress"),
    PrivateKeyHex = GetFirstSetting("ETH_PRIVATE_KEY", "Ethereum:PrivateKeyHex"),
    PollIntervalSeconds = GetNullableIntFromMany("ETH_POLL_INTERVAL_SECONDS", "Ethereum:PollIntervalSeconds") ?? 5,
    ReconnectDelaySeconds = GetNullableIntFromMany("ETH_RECONNECT_DELAY_SECONDS", "Ethereum:ReconnectDelaySeconds") ?? 5
};
var crossChainOptions = GetCrossChainOptions();
var cacheSyncOptions = GetCacheSyncOptions();
var resyncOptions = GetResyncOptions();
var services = new ServiceCollection();
services.AddLogging(builder =>
{
    builder.SetMinimumLevel(logLevel);
    if (useJsonLogging)
    {
        builder.AddJsonConsole(options =>
        {
            options.TimestampFormat = "O";
            options.IncludeScopes = true;
        });
    }
    else
    {
        builder.AddSimpleConsole(options =>
        {
            options.TimestampFormat = "O ";
            options.IncludeScopes = true;
        });
    }
});
services.AddOptions();
services.AddSingleton<IOptions<CacheOptions>>(Options.Create(cacheOptions));
services.AddSingleton<IOptions<CompressionOptions>>(Options.Create(compressionOptions));
services.AddSingleton<IOptions<EncryptionOptions>>(Options.Create(encryptionOptions));
services.AddSingleton<IOptions<IntegrityOptions>>(Options.Create(integrityOptions));
services.AddSingleton<IOptions<AclOptions>>(Options.Create(aclOptions));
services.AddSingleton<IOptions<AsyncProcessingOptions>>(Options.Create(asyncProcessingOptions));
services.AddSingleton<IOptions<SwarmKeyDbOptions>>(Options.Create(privacyOptions));
services.AddSingleton<IOptions<CacheSyncOptions>>(Options.Create(cacheSyncOptions));
services.AddSingleton<IOptions<ResyncOptions>>(Options.Create(resyncOptions));
services.AddSingleton<IEncryptionKeyProvider>(_ => new MutableEncryptionKeyProvider(encryptionOptions));
services.AddSingleton<IResyncMetricsReporter>(_ => monitoringMetrics);
services.AddSingleton<IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
services.AddSingleton<IEthAddressAccessor, AsyncLocalEthAddressAccessor>();
services.AddSingleton<IDidContextAccessor, AsyncLocalDidContextAccessor>();
if (didMode != DidAuthMode.None)
{
    services.AddSingleton<IDecentralizedIdentityProvider>(
        _ => new EthrDidProvider(new EthrDidProviderOptions { RpcUrl = didRpcUrl }));
}
services.AddSingleton<ICacheSyncBus>(sp =>
{
    if (!cacheSyncOptions.Enabled || cacheSyncOptions.Peers.Count == 0)
    {
        return NoOpCacheSyncBus.Instance;
    }

    var logger = sp.GetRequiredService<ILogger<RedisCacheSyncBus>>();
    try
    {
        return new RedisCacheSyncBus(cacheSyncOptions.Peers, cacheSyncOptions.Channel, logger);
    }
    catch (Exception ex)
    {
        logger.LogWarning(
            ex,
            "Cache sync bus initialization failed; continuing with local-only cache mode.");
        return NoOpCacheSyncBus.Instance;
    }
});
IReadinessProbe readinessProbe;
IShardHealthProvider? shardHealthProvider = null;
IBackendStatusProvider? backendStatusProvider = null;
IConnectivityProbe connectivityProbe = new AlwaysConnectedProbe();
var ownedResources = new List<IDisposable>();
ISwarmClient? snapshotSwarmClient = null;

if (shardingOptions.Enabled)
{
    if (backend is "ipfs" or "hybrid")
    {
        throw new InvalidOperationException("Sharding currently supports only local/swarm backends.");
    }

    var shardStores = new List<ShardStore>();
    var shardReadiness = new List<ShardReadinessRegistration>();
    for (var i = 0; i < shardingOptions.Nodes.Count; i++)
    {
        var node = shardingOptions.Nodes[i];
        var shardName = string.IsNullOrWhiteSpace(node.Name) ? $"shard-{i + 1}" : node.Name;
        var shardDataDir = string.IsNullOrWhiteSpace(node.DataDir)
            ? Path.Combine(dataDir, "shards", shardName)
            : node.DataDir;
        var shardIndex = BuildKeyIndex(Path.Combine(shardDataDir, "index.json"));
        ISwarmClient shardClient;
        IReadinessProbe shardProbe;
        if (backend.Equals("bee", StringComparison.OrdinalIgnoreCase) ||
            backend.Equals("swarm", StringComparison.OrdinalIgnoreCase) ||
            !string.IsNullOrWhiteSpace(node.BeeUrl))
        {
            var beeUrl = new Uri(node.BeeUrl ?? GetString("BEE_URL", "http://localhost:1633/"));
            var batchId = node.PostageBatchId
                ?? GetSetting("BEE_POSTAGE_BATCH_ID")
                ?? throw new InvalidOperationException("Configuration value BEE_POSTAGE_BATCH_ID is required.");
            var probe = new BeeReadinessProbe(beeUrl, batchId);
            shardProbe = probe;
            shardClient = new BeeSwarmClient(beeUrl, batchId);
            ownedResources.Add(probe);
            ownedResources.Add((IDisposable)shardClient);
        }
        else
        {
            shardProbe = new AlwaysReadyProbe();
            shardClient = new FileSwarmClient(Path.Combine(shardDataDir, "objects"));
        }

        var instrumentedShardClient = new InstrumentedSwarmClient(shardClient, monitoringMetrics);
        snapshotSwarmClient ??= instrumentedShardClient;
        var shardStore = new SwarmKeyValueStore(instrumentedShardClient, shardIndex, integrityOptions);
        shardStores.Add(new ShardStore(shardName, shardStore));
        shardReadiness.Add(new ShardReadinessRegistration(shardName, shardProbe, shardStore));
    }

    var compositeProbe = new CompositeShardReadinessProbe(shardReadiness);
    readinessProbe = compositeProbe;
    shardHealthProvider = compositeProbe;
    backendStatusProvider = new CompositeBackendStatusProvider(
    [
        ("swarm", new AnyReadyProbe(shardReadiness.Select(static registration => (registration.Shard, registration.Probe)).ToArray()))
    ]);
    services.AddSwarmKeyDbStore(_ => new ShardingRouter(
        shardStores,
        shardingOptions.ShardCount,
        shardingOptions.VirtualNodesPerNode));
}
else
{
    var index = BuildKeyIndex(Path.Combine(dataDir, "index.json"));
    var backendProbes = new List<(string Name, IReadinessProbe Probe)>();
    switch (backend)
    {
        case "bee":
        case "swarm":
        {
            var beeUrl = new Uri(GetString("BEE_URL", "http://localhost:1633/"));
            var batchId = RequireSetting("BEE_POSTAGE_BATCH_ID");
            var swarmProbe = new BeeReadinessProbe(beeUrl, batchId);
            var swarmClient = new BeeSwarmClient(beeUrl, batchId);
            var instrumentedClient = new InstrumentedSwarmClient(swarmClient, monitoringMetrics);
            snapshotSwarmClient = instrumentedClient;
            readinessProbe = swarmProbe;
            connectivityProbe = new HttpHealthConnectivityProbe(beeUrl);
            backendProbes.Add(("swarm", swarmProbe));
            ownedResources.Add(swarmProbe);
            ownedResources.Add((IDisposable)connectivityProbe);
            ownedResources.Add((IDisposable)swarmClient);
            services.AddSwarmKeyDbStore(_ => new SwarmKeyValueStore(instrumentedClient, index, integrityOptions));
            if (consistencyEnabled)
            {
                services.AddSwarmConsistency(
                    ResolveConsistencyBeeNodes(beeUrl, consistencyBeeNodesRaw),
                    options =>
                    {
                        options.Enabled = true;
                        options.FailureMode = consistencyFailureMode;
                        options.QuorumThreshold = consistencyQuorumThreshold;
                        options.FeedOwner = consistencyFeedOwner;
                    });
            }
            break;
        }
        case "ipfs":
        {
            var ipfsProbe = new IpfsReadinessProbe(new Uri(ipfsApiUrl));
            var ipfsClient = new IpfsSwarmClient(new Uri(ipfsApiUrl), ipfsPinOnWrite);
            var instrumentedIpfsClient = new InstrumentedSwarmClient(ipfsClient, monitoringMetrics);
            snapshotSwarmClient = instrumentedIpfsClient;
            readinessProbe = ipfsProbe;
            backendProbes.Add(("ipfs", ipfsProbe));
            ownedResources.Add(ipfsProbe);
            ownedResources.Add(ipfsClient);
            services.AddSwarmKeyDbStore(_ => new IpfsStorageBackend(instrumentedIpfsClient, index, integrityOptions));
            break;
        }
        case "hybrid":
        {
            ISwarmClient swarmClient;
            IReadinessProbe swarmProbe;
            if (!string.IsNullOrWhiteSpace(GetSetting("BEE_POSTAGE_BATCH_ID")))
            {
                var beeUrl = new Uri(GetString("BEE_URL", "http://localhost:1633/"));
                var batchId = RequireSetting("BEE_POSTAGE_BATCH_ID");
                var beeProbe = new BeeReadinessProbe(beeUrl, batchId);
                swarmProbe = beeProbe;
                swarmClient = new BeeSwarmClient(beeUrl, batchId);
                connectivityProbe = new HttpHealthConnectivityProbe(beeUrl);
                ownedResources.Add(beeProbe);
                ownedResources.Add((IDisposable)connectivityProbe);
                ownedResources.Add((IDisposable)swarmClient);
            }
            else
            {
                swarmProbe = new AlwaysReadyProbe();
                swarmClient = new FileSwarmClient(Path.Combine(dataDir, "objects"));
            }

            var ipfsProbe = new IpfsReadinessProbe(new Uri(ipfsApiUrl));
            var ipfsClient = new IpfsSwarmClient(new Uri(ipfsApiUrl), ipfsPinOnWrite);
            var instrumentedSwarmClient = new InstrumentedSwarmClient(swarmClient, monitoringMetrics);
            var instrumentedIpfsClient = new InstrumentedSwarmClient(ipfsClient, monitoringMetrics);
            var hybridClient = new HybridSwarmClient(instrumentedSwarmClient, instrumentedIpfsClient);
            snapshotSwarmClient = hybridClient;
            readinessProbe = new AnyReadyProbe([("swarm", swarmProbe), ("ipfs", ipfsProbe)]);
            backendProbes.Add(("swarm", swarmProbe));
            backendProbes.Add(("ipfs", ipfsProbe));
            ownedResources.Add(ipfsProbe);
            ownedResources.Add(ipfsClient);
            services.AddSwarmKeyDbStore(_ => new HybridBackend(instrumentedSwarmClient, instrumentedIpfsClient, index, integrityOptions));
            break;
        }
        default:
        {
            readinessProbe = new AlwaysReadyProbe();
            var swarmClient = new FileSwarmClient(Path.Combine(dataDir, "objects"));
            var instrumentedClient = new InstrumentedSwarmClient(swarmClient, monitoringMetrics);
            snapshotSwarmClient = instrumentedClient;
            backendProbes.Add(("local", readinessProbe));
            services.AddSwarmKeyDbStore(_ => new SwarmKeyValueStore(instrumentedClient, index, integrityOptions));
            break;
        }
    }

    backendStatusProvider = new CompositeBackendStatusProvider(backendProbes);
}

services.AddSingleton<ISwarmClient>(_ => snapshotSwarmClient ?? throw new InvalidOperationException("No Swarm client is configured."));
services.AddSingleton<IConnectivityProbe>(connectivityProbe);

services.AddSingleton<BackupService>(sp => new BackupService(
    sp.GetRequiredService<IKeyValueStore>(),
    sp.GetRequiredService<ISwarmClient>(),
    sp.GetService<IEncryptionKeyProvider>()));
services.AddSingleton<RestoreService>(sp => new RestoreService(
    sp.GetRequiredService<BackupService>(),
    sp.GetRequiredService<IKeyValueStore>(),
    sp.GetService<IEncryptionKeyProvider>()));
services.AddSingleton<KeyRotationService>(sp => new KeyRotationService(
    sp.GetRequiredService<IKeyValueStore>(),
    sp.GetRequiredService<ISwarmClient>(),
    sp.GetRequiredService<IEncryptionKeyProvider>(),
    sp.GetRequiredService<BackupService>()));
if (privacyOptions.OfflineMode != OfflineMode.Never)
{
    services.AddSingleton<OfflineSyncService>(sp =>
        new OfflineSyncService(
            (IOfflineKeyValueStore)sp.GetRequiredService<IKeyValueStore>(),
            TimeSpan.FromMilliseconds(privacyOptions.OfflineSyncIntervalMs),
            sp.GetRequiredService<ILogger<OfflineSyncService>>()));
}
services.AddSingleton<ICacheSyncStatusProvider>(sp =>
{
    if (!cacheSyncOptions.Enabled || sp.GetRequiredService<IKeyValueStore>() is not ICacheSyncParticipant participant)
    {
        return NoOpCacheSyncStatusProvider.Instance;
    }

    return new AntiEntropyService(
        participant,
        sp.GetRequiredService<ICacheSyncBus>(),
        cacheSyncOptions,
        sp.GetRequiredService<ILogger<AntiEntropyService>>());
});
services.AddSingleton<IResyncCoordinator>(sp =>
{
    if (sp.GetRequiredService<IKeyValueStore>() is not ICacheSyncParticipant participant)
    {
        return NoOpResyncCoordinator.Instance;
    }

    return new ResyncCoordinator(
        participant,
        sp.GetRequiredService<IKeyValueStore>(),
        sp.GetRequiredService<ICacheSyncBus>(),
        cacheSyncOptions,
        resyncOptions,
        sp.GetRequiredService<ILogger<ResyncCoordinator>>(),
        sp.GetRequiredService<IResyncMetricsReporter>());
});
services.AddSingleton<IResyncStatusProvider>(sp => sp.GetRequiredService<IResyncCoordinator>());

using var provider = services.BuildServiceProvider();
cacheStats = provider.GetRequiredService<ICacheStats>();
consistencyStatusMetrics = provider.GetService<IConsistencyVerificationStatusProvider>();
var baseStore = provider.GetRequiredService<IKeyValueStore>();
var offlineStatusProvider = provider.GetRequiredService<IOfflineStatusProvider>();
var consistencyStatusProvider = provider.GetRequiredService<IConsistencyVerificationStatusProvider>();
var cacheSyncStatusProvider = provider.GetService<ICacheSyncStatusProvider>() ?? NoOpCacheSyncStatusProvider.Instance;
var resyncCoordinator = provider.GetService<IResyncCoordinator>() ?? NoOpResyncCoordinator.Instance;
var resyncStatusProvider = provider.GetService<IResyncStatusProvider>() ?? NoOpResyncCoordinator.Instance;
offlineStatusMetrics = offlineStatusProvider;
cacheSyncStatusMetrics = cacheSyncStatusProvider;
// Create the Pub/Sub manager (singleton shared across all connections)
pubSubManager = new PubSubManager(
    provider.GetService<ICacheSyncBus>(),
    nodeId: null,
    provider.GetService<ILogger<PubSubManager>>());
CrossChainSyncService? crossChainSyncService = null;
OfflineSyncService? offlineSyncService = provider.GetService<OfflineSyncService>();
AntiEntropyService? antiEntropyService = cacheSyncStatusProvider as AntiEntropyService;
IKeyValueStore commandStore = baseStore;
if (crossChainOptions.Enabled && crossChainOptions.Chains.Count > 0)
{
    var adapters = crossChainOptions.Chains.Select(chain => (IChainAdapter)new NamespacedChainAdapter(baseStore, chain)).ToArray();
    crossChainSyncService = new CrossChainSyncService(
        adapters,
        new FileCrossChainStateStore(Path.Combine(dataDir, "crosschain-sync.json")),
        crossChainOptions,
        provider.GetRequiredService<ILogger<CrossChainSyncService>>());
    commandStore = new CrossChainReplicatingKeyValueStore(baseStore, crossChainSyncService, crossChainOptions.DefaultChainIds);
}

var processor = new RedisCommandProcessor(
    commandStore,
    provider.GetRequiredService<IEthAddressAccessor>(),
    provider.GetRequiredService<BackupService>(),
    provider.GetRequiredService<RestoreService>(),
    provider.GetRequiredService<KeyRotationService>(),
    monitoringMetrics,
    provider.GetRequiredService<ILogger<RedisCommandProcessor>>(),
    provider.GetRequiredService<IDidContextAccessor>(),
    provider.GetService<IDecentralizedIdentityProvider>(),
    resyncCoordinator,
    pubSubManager);
var server = new RedisServer(
    bind,
    port,
    processor,
    monitoringMetrics.OnConnectionOpened,
    monitoringMetrics.OnConnectionClosed,
    provider.GetRequiredService<ILogger<RedisServer>>());

// Create the Ethereum bridge (opt-in: only active when ETH_BRIDGE_ENABLED=true)
EthereumBridgeService? ethereumBridge = null;
if (ethereumBridgeOptions.Enabled)
{
    ethereumBridge = new EthereumBridgeService(
        provider.GetRequiredService<IKeyValueStore>(),
        ethereumBridgeOptions,
        provider.GetRequiredService<ILogger<EthereumBridgeService>>());
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

if (ethereumBridge is not null)
{
    await ethereumBridge.StartAsync(cts.Token);
}

if (crossChainSyncService is not null)
{
    await crossChainSyncService.StartAsync(cts.Token);
}

if (offlineSyncService is not null)
{
    await offlineSyncService.StartAsync(cts.Token);
}

if (antiEntropyService is not null)
{
    await antiEntropyService.StartAsync(cts.Token);
}

if (!ReferenceEquals(resyncCoordinator, NoOpResyncCoordinator.Instance))
{
    try
    {
        _ = await resyncCoordinator.TriggerResyncAsync(ResyncMode.Auto, cts.Token);
    }
    catch (Exception ex)
    {
        provider.GetRequiredService<ILoggerFactory>()
            .CreateLogger("ResyncStartup")
            .LogWarning(ex, "Startup resync failed. Continuing server startup.");
    }
}

var monitoringServers = new List<MonitoringHttpServer>();
var monitoringTasks = new List<Task>();
if (dashboardEnabled)
{
    var dashboardServer = new MonitoringHttpServer(
        bind,
        dashboardPort,
        monitoringMetrics,
        readinessProbe,
        metricsEnabled: metricsEnabled,
        dashboardEnabled: true,
        provider.GetRequiredService<ILogger<MonitoringHttpServer>>(),
        shardHealthProvider,
        backendStatusProvider,
        ethereumBridge,
        crossChainSyncService,
        offlineStatusProvider,
        consistencyStatusProvider,
        cacheSyncStatusProvider,
        resyncStatusProvider,
        resyncCoordinator,
        privacyMode: privacyOptions.PrivacyMode,
        didMode: privacyOptions.DidMode);
    monitoringServers.Add(dashboardServer);
    monitoringTasks.Add(dashboardServer.RunAsync(cts.Token));
}

if (metricsEnabled && (!dashboardEnabled || metricsPort != dashboardPort))
{
    var metricsServer = new MonitoringHttpServer(
        bind,
        metricsPort,
        monitoringMetrics,
        readinessProbe,
        metricsEnabled: true,
        dashboardEnabled: false,
        provider.GetRequiredService<ILogger<MonitoringHttpServer>>(),
        shardHealthProvider,
        backendStatusProvider,
        ethereumBridge,
        crossChainSyncService,
        offlineStatusProvider,
        consistencyStatusProvider,
        cacheSyncStatusProvider,
        resyncStatusProvider,
        resyncCoordinator,
        privacyMode: privacyOptions.PrivacyMode,
        didMode: privacyOptions.DidMode);
    monitoringServers.Add(metricsServer);
    monitoringTasks.Add(metricsServer.RunAsync(cts.Token));
}

await server.RunAsync(cts.Token);
await Task.WhenAll(monitoringTasks);

if (ethereumBridge is not null)
{
    await ethereumBridge.DisposeAsync();
}

if (crossChainSyncService is not null)
{
    await crossChainSyncService.DisposeAsync();
}

if (offlineSyncService is not null)
{
    await offlineSyncService.DisposeAsync();
}

if (antiEntropyService is not null)
{
    await antiEntropyService.DisposeAsync();
}

foreach (var monitoringServer in monitoringServers)
{
    monitoringServer.Dispose();
}
foreach (var resource in ownedResources)
{
    resource.Dispose();
}

int GetInt(string name, int defaultValue) =>
    int.TryParse(GetSetting(name), out var value) ? value : defaultValue;

int? GetNullableInt(string name) =>
    int.TryParse(GetSetting(name), out var value) ? value : null;

bool GetBool(string name, bool defaultValue) =>
    bool.TryParse(GetSetting(name), out var value) ? value : defaultValue;

LogLevel GetLogLevel(string name, LogLevel defaultValue) =>
    Enum.TryParse<LogLevel>(GetSetting(name), ignoreCase: true, out var level) ? level : defaultValue;

string GetString(string name, string defaultValue) => GetSetting(name) ?? defaultValue;

string RequireSetting(string name) =>
    GetSetting(name) ?? throw new InvalidOperationException($"Configuration value {name} is required.");

string? GetSetting(string name) =>
    Environment.GetEnvironmentVariable(name) ?? (appSettings.TryGetValue(name, out var value) ? value : null);

PrivacyMode GetPrivacyModeFromSettings()
{
    var configured = GetFirstSetting("SWARM_KEYDB_PRIVACY_MODE", "Privacy:Mode");
    if (Enum.TryParse<PrivacyMode>(configured, ignoreCase: true, out var explicitMode))
    {
        return explicitMode;
    }

    return PrivacyMode.None;
}

DidAuthMode GetDidModeFromSettings()
{
    var configured = GetFirstSetting("SWARM_KEYDB_DID_MODE", "Did:Mode");
    if (Enum.TryParse<DidAuthMode>(configured, ignoreCase: true, out var mode))
    {
        return mode;
    }

    return DidAuthMode.None;
}

OfflineMode GetOfflineModeFromSettings()
{
    var configured = GetFirstSetting("SWARM_KEYDB_OFFLINE_MODE", "Offline:Mode");
    if (Enum.TryParse<OfflineMode>(configured, ignoreCase: true, out var mode))
    {
        return mode;
    }

    return OfflineMode.Never;
}

OfflineJournalType GetOfflineJournalFromSettings()
{
    var configured = GetFirstSetting("SWARM_KEYDB_OFFLINE_JOURNAL", "Offline:Journal");
    if (Enum.TryParse<OfflineJournalType>(configured, ignoreCase: true, out var journal))
    {
        return journal;
    }

    return OfflineJournalType.Memory;
}

IKeyIndex BuildKeyIndex(string path)
{
    var baseIndex = new FileKeyIndex(path);
    if (privacyOptions.PrivacyMode == PrivacyMode.None)
    {
        return baseIndex;
    }

    var strategy = KeyPrivacyStrategyFactory.Create(privacyOptions);
    return new PrivacyPreservingKeyIndex(baseIndex, strategy);
}

ShardingOptions GetShardingOptions()
{
    var enabled = GetBoolFromMany(defaultValue: false, "SWARM_KEYDB_SHARDING_ENABLED", "Sharding:Enabled");
    var nodes = ParseShardNodes(GetFirstSetting("SWARM_KEYDB_SHARDING_NODES", "Sharding:Nodes"));
    var shardCount = GetNullableIntFromMany("SWARM_KEYDB_SHARDING_SHARD_COUNT", "Sharding:ShardCount")
        ?? Math.Max(1, nodes.Count);

    return new ShardingOptions
    {
        Enabled = enabled,
        Nodes = nodes,
        ShardCount = shardCount,
        VirtualNodesPerNode = GetNullableIntFromMany("SWARM_KEYDB_SHARDING_VIRTUAL_NODES", "Sharding:VirtualNodesPerNode") ?? 128
    };
}

IReadOnlyList<ShardNodeOptions> ParseShardNodes(string? json)
{
    if (string.IsNullOrWhiteSpace(json))
    {
        return [];
    }

    try
    {
        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var nodes = new List<ShardNodeOptions>();
        var index = 1;
        foreach (var element in document.RootElement.EnumerateArray())
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                {
                    var endpoint = element.GetString();
                    if (!string.IsNullOrWhiteSpace(endpoint))
                    {
                        nodes.Add(new ShardNodeOptions
                        {
                            Name = $"shard-{index++}",
                            BeeUrl = endpoint
                        });
                    }

                    break;
                }
                case JsonValueKind.Object:
                {
                    var name = element.TryGetProperty("name", out var nameProperty) ? nameProperty.GetString() : null;
                    var beeUrl = element.TryGetProperty("beeUrl", out var beeUrlProperty) ? beeUrlProperty.GetString() : null;
                    var postageBatchId = element.TryGetProperty("postageBatchId", out var batchProperty) ? batchProperty.GetString() : null;
                    var shardDataDir = element.TryGetProperty("dataDir", out var dataDirProperty) ? dataDirProperty.GetString() : null;
                    nodes.Add(new ShardNodeOptions
                    {
                        Name = string.IsNullOrWhiteSpace(name) ? $"shard-{index}" : name!,
                        BeeUrl = beeUrl,
                        PostageBatchId = postageBatchId,
                        DataDir = shardDataDir
                    });
                    index++;
                    break;
                }
            }
        }

        return nodes;
    }
    catch (JsonException ex)
    {
        throw new InvalidOperationException(
            "Sharding nodes configuration is invalid. Configure SWARM_KEYDB_SHARDING_NODES as a JSON array of Bee URLs or node objects.",
            ex);
    }
}

string? GetFirstSetting(params string[] names)
{
    foreach (var name in names)
    {
        var value = GetSetting(name);
        if (value is not null)
        {
            return value;
        }
    }

    return null;
}

int? GetNullableIntFromMany(params string[] names)
{
    foreach (var name in names)
    {
        if (int.TryParse(GetSetting(name), out var value))
        {
            return value;
        }
    }

    return null;
}

bool GetBoolFromMany(bool defaultValue, params string[] names)
{
    foreach (var name in names)
    {
        if (bool.TryParse(GetSetting(name), out var value))
        {
            return value;
        }
    }

    return defaultValue;
}

IReadOnlyList<Uri> ResolveConsistencyBeeNodes(Uri defaultBeeUrl, string? raw)
{
    if (string.IsNullOrWhiteSpace(raw))
    {
        return [defaultBeeUrl];
    }

    var values = raw
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(static value => new Uri(value))
        .ToArray();
    return values.Length == 0 ? [defaultBeeUrl] : values;
}

AclOptions GetAclOptions()
{
    var enabled = GetBool("SWARM_KEYDB_ACL_ENABLED", false);
    var modeText = GetSetting("SWARM_KEYDB_ACL_MODE");
    var mode = string.IsNullOrWhiteSpace(modeText)
        ? AclMode.Allowlist
        : Enum.TryParse<AclMode>(modeText, ignoreCase: true, out var parsedMode)
            ? parsedMode
            : throw new InvalidOperationException("SWARM_KEYDB_ACL_MODE must be 'allowlist' or 'denylist'.");

    var entriesJson = GetSetting("SWARM_KEYDB_ACL_ENTRIES");
    if (string.IsNullOrWhiteSpace(entriesJson))
    {
        return new AclOptions
        {
            Enabled = enabled,
            Mode = mode,
            Entries = []
        };
    }

    try
    {
        var entries = JsonSerializer.Deserialize<List<AclEntry>>(entriesJson, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        }) ?? [];

        return new AclOptions
        {
            Enabled = enabled,
            Mode = mode,
            Entries = entries
        };
    }
    catch (JsonException ex)
    {
        if (!enabled)
        {
            return new AclOptions
            {
                Enabled = false,
                Mode = mode,
                Entries = []
            };
        }

        throw new InvalidOperationException(
            "ACL is enabled (SWARM_KEYDB_ACL_ENABLED=true) but SWARM_KEYDB_ACL_ENTRIES is not valid JSON. " +
            "Configure a JSON array of {\"address\":\"0x...\",\"permission\":\"read|write|admin\"} entries.",
            ex);
    }
}

CrossChainOptions GetCrossChainOptions()
{
    var options = new CrossChainOptions
    {
        Enabled = GetBoolFromMany(defaultValue: false, "SWARM_KEYDB_CROSS_CHAIN_ENABLED", "CrossChain:Enabled"),
        MaxRetryAttempts = GetNullableIntFromMany("SWARM_KEYDB_CROSS_CHAIN_MAX_RETRIES", "CrossChain:MaxRetryAttempts") ?? 5,
        RetryBaseDelaySeconds = GetNullableIntFromMany("SWARM_KEYDB_CROSS_CHAIN_RETRY_BASE_SECONDS", "CrossChain:RetryBaseDelaySeconds") ?? 5,
        MaxRetryDelaySeconds = GetNullableIntFromMany("SWARM_KEYDB_CROSS_CHAIN_MAX_RETRY_DELAY_SECONDS", "CrossChain:MaxRetryDelaySeconds") ?? 300,
        ReconcileIntervalSeconds = GetNullableIntFromMany("SWARM_KEYDB_CROSS_CHAIN_RECONCILE_SECONDS", "CrossChain:ReconcileIntervalSeconds") ?? 5,
        DefaultChainIds = ParseIntArray(GetFirstSetting("SWARM_KEYDB_CROSS_CHAIN_DEFAULT_CHAIN_IDS", "CrossChain:DefaultChainIds")).ToList(),
        Chains = ParseCrossChainAdapters(GetFirstSetting("SWARM_KEYDB_CROSS_CHAIN_CHAINS", "CrossChain:Chains")).ToList()
    };

    return options;
}

CacheSyncOptions GetCacheSyncOptions()
{
    var peers = ParseStringArray(GetFirstSetting("SWARM_KEYDB_SYNC_PEERS", "CacheSync:Peers"));
    return new CacheSyncOptions
    {
        Enabled = peers.Count > 0,
        Peers = peers,
        SyncIntervalSeconds = Math.Max(1, GetNullableIntFromMany("SWARM_KEYDB_SYNC_INTERVAL_SEC", "CacheSync:IntervalSec") ?? 5),
        Channel = GetFirstSetting("SWARM_KEYDB_SYNC_CHANNEL", "CacheSync:Channel") ?? "swarm-keydb-sync",
        NodeId = GetFirstSetting("SWARM_KEYDB_SYNC_NODE_ID", "CacheSync:NodeId")
                 ?? $"{Environment.MachineName}:{port}"
    };
}

ResyncOptions GetResyncOptions()
{
    var modeText = GetFirstSetting("SWARM_KEYDB_RESYNC_MODE", "Resync:Mode");
    var mode = string.IsNullOrWhiteSpace(modeText)
        ? ResyncMode.Auto
        : Enum.TryParse<ResyncMode>(modeText, ignoreCase: true, out var parsedMode)
            ? parsedMode
            : throw new InvalidOperationException("SWARM_KEYDB_RESYNC_MODE must be auto, partial, or full.");

    return new ResyncOptions
    {
        Mode = mode,
        MaxVersionGapForPartialResync = Math.Max(0, GetNullableIntFromMany("SWARM_KEYDB_RESYNC_MAX_VERSION_GAP", "Resync:MaxVersionGapForPartialResync") ?? 128),
        FullResyncBatchSize = Math.Max(1, GetNullableIntFromMany("SWARM_KEYDB_RESYNC_FULL_BATCH_SIZE", "Resync:FullResyncBatchSize") ?? 256),
        ResyncTimeoutSeconds = Math.Max(1, GetNullableIntFromMany("SWARM_KEYDB_RESYNC_TIMEOUT_SECONDS", "Resync:TimeoutSeconds") ?? 30)
    };
}

IReadOnlyList<int> ParseIntArray(string? json)
{
    if (string.IsNullOrWhiteSpace(json))
    {
        return [];
    }

    try
    {
        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind == JsonValueKind.Array)
        {
            return document.RootElement.EnumerateArray()
                .Select(static element => element.ValueKind switch
                {
                    JsonValueKind.Number => element.GetInt32(),
                    JsonValueKind.String when int.TryParse(element.GetString(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var value) => value,
                    _ => throw new InvalidOperationException("Cross-chain default chain ids must be numbers or numeric strings.")
                })
                .ToArray();
        }
    }
    catch (JsonException)
    {
    }

    return json.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(static value => int.Parse(value, System.Globalization.CultureInfo.InvariantCulture))
        .ToArray();
}

IReadOnlyList<string> ParseStringArray(string? raw)
{
    if (string.IsNullOrWhiteSpace(raw))
    {
        return [];
    }

    try
    {
        using var document = JsonDocument.Parse(raw);
        if (document.RootElement.ValueKind == JsonValueKind.Array)
        {
            return document.RootElement.EnumerateArray()
                .Where(static element => element.ValueKind == JsonValueKind.String)
                .Select(static element => element.GetString())
                .Where(static value => !string.IsNullOrWhiteSpace(value))
                .Cast<string>()
                .ToArray();
        }
    }
    catch (JsonException)
    {
    }

    return raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}

IReadOnlyList<ChainAdapterOptions> ParseCrossChainAdapters(string? json)
{
    if (string.IsNullOrWhiteSpace(json))
    {
        return [];
    }

    try
    {
        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var adapters = new List<ChainAdapterOptions>();
        foreach (var element in document.RootElement.EnumerateArray())
        {
            if (element.ValueKind == JsonValueKind.Number)
            {
                adapters.Add(new ChainAdapterOptions
                {
                    ChainId = element.GetInt32(),
                    Name = $"chain-{element.GetInt32()}"
                });
                continue;
            }

            if (element.ValueKind == JsonValueKind.Object)
            {
                static string? GetString(JsonElement element, string primary, string alternate) =>
                    element.TryGetProperty(primary, out var property) ? property.GetString()
                    : element.TryGetProperty(alternate, out property) ? property.GetString()
                    : null;

                static int GetInt(JsonElement element, string primary, string alternate)
                {
                    if (element.TryGetProperty(primary, out var property) || element.TryGetProperty(alternate, out property))
                    {
                        return property.GetInt32();
                    }

                    throw new KeyNotFoundException(primary);
                }

                adapters.Add(new ChainAdapterOptions
                {
                    ChainId = GetInt(element, "chainId", "ChainId"),
                    Name = GetString(element, "name", "Name") ?? string.Empty,
                    RpcUrl = GetString(element, "rpcUrl", "RpcUrl"),
                    BridgeContractAddress = GetString(element, "bridgeContractAddress", "BridgeContractAddress")
                });
            }
        }

        return adapters;
    }
    catch (JsonException ex)
    {
        throw new InvalidOperationException(
            "Cross-chain configuration is invalid. Configure CrossChain:Chains as a JSON array of chain ids or objects with chainId, name, rpcUrl, and bridgeContractAddress.",
            ex);
    }
}

Dictionary<string, string> LoadAppSettings()
{
    var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
    if (!File.Exists(path))
    {
        path = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.json");
        if (!File.Exists(path))
        {
            return settings;
        }
    }

    using var stream = File.OpenRead(path);
    using var document = JsonDocument.Parse(stream);
    FlattenJson(document.RootElement, settings, prefix: null);
    return settings;
}

void FlattenJson(JsonElement element, Dictionary<string, string> destination, string? prefix)
{
    if (element.ValueKind != JsonValueKind.Object)
    {
        return;
    }

    foreach (var property in element.EnumerateObject())
    {
        var key = string.IsNullOrEmpty(prefix) ? property.Name : $"{prefix}:{property.Name}";
        if (property.Value.ValueKind == JsonValueKind.Object)
        {
            FlattenJson(property.Value, destination, key);
            continue;
        }

        destination[key] = property.Value.ToString();
    }
}
