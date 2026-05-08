using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;
using SwarmKeyDb;
using SwarmKeyDb.Server;

var appSettings = LoadAppSettings();
var port = GetInt("SWARM_KEYDB_PORT", 6379);
var bind = IPAddress.Parse(GetString("SWARM_KEYDB_BIND", "0.0.0.0"));
var dataDir = GetString("SWARM_KEYDB_DATA_DIR", Path.Combine(AppContext.BaseDirectory, "data"));
var backend = GetString("SWARM_KEYDB_BACKEND", "local");
var metricsEnabled = GetBool("METRICS_ENABLED", true);
var metricsPort = GetInt("METRICS_PORT", 9090);
var dashboardEnabled = GetBool("DASHBOARD_ENABLED", true);
var dashboardPort = GetInt("DASHBOARD_PORT", 8080);

var logLevel = GetLogLevel("LOG_LEVEL", GetLogLevel("SWARM_KEYDB_LOG_LEVEL", LogLevel.Information));
var environment = GetString("DOTNET_ENVIRONMENT", GetString("ASPNETCORE_ENVIRONMENT", "Production"));
var useJsonLogging = GetBool("JSON_LOGS", !environment.Equals("Development", StringComparison.OrdinalIgnoreCase));

ICacheStats? cacheStats = null;
var monitoringMetrics = new MonitoringMetrics(() => cacheStats ?? NoOpCacheStats.Instance);

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
services.AddSingleton<IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
services.AddSingleton<IEthAddressAccessor, AsyncLocalEthAddressAccessor>();
IReadinessProbe readinessProbe;
IShardHealthProvider? shardHealthProvider = null;
var ownedResources = new List<IDisposable>();

if (shardingOptions.Enabled)
{
    var shardStores = new List<ShardStore>();
    var shardReadiness = new List<ShardReadinessRegistration>();
    for (var i = 0; i < shardingOptions.Nodes.Count; i++)
    {
        var node = shardingOptions.Nodes[i];
        var shardName = string.IsNullOrWhiteSpace(node.Name) ? $"shard-{i + 1}" : node.Name;
        var shardDataDir = string.IsNullOrWhiteSpace(node.DataDir)
            ? Path.Combine(dataDir, "shards", shardName)
            : node.DataDir;
        var shardIndex = new FileKeyIndex(Path.Combine(shardDataDir, "index.json"));
        ISwarmClient shardClient;
        IReadinessProbe shardProbe;
        if (backend.Equals("bee", StringComparison.OrdinalIgnoreCase) || !string.IsNullOrWhiteSpace(node.BeeUrl))
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

        var instrumentedClient = new InstrumentedSwarmClient(shardClient, monitoringMetrics);
        var shardStore = new SwarmKeyValueStore(swarmClient: instrumentedClient, index: shardIndex, integrityOptions: integrityOptions);
        shardStores.Add(new ShardStore(shardName, shardStore));
        shardReadiness.Add(new ShardReadinessRegistration(shardName, shardProbe, shardStore));
    }

    var compositeProbe = new CompositeShardReadinessProbe(shardReadiness);
    readinessProbe = compositeProbe;
    shardHealthProvider = compositeProbe;
    services.AddSwarmKeyDbStore(_ => new ShardingRouter(
        shardStores,
        shardingOptions.ShardCount,
        shardingOptions.VirtualNodesPerNode));
}
else
{
    var index = new FileKeyIndex(Path.Combine(dataDir, "index.json"));
    ISwarmClient swarmClient;
    if (backend.Equals("bee", StringComparison.OrdinalIgnoreCase))
    {
        var beeUrl = new Uri(GetString("BEE_URL", "http://localhost:1633/"));
        var batchId = RequireSetting("BEE_POSTAGE_BATCH_ID");
        var probe = new BeeReadinessProbe(beeUrl, batchId);
        readinessProbe = probe;
        swarmClient = new BeeSwarmClient(beeUrl, batchId);
        ownedResources.Add(probe);
        ownedResources.Add((IDisposable)swarmClient);
    }
    else
    {
        readinessProbe = new AlwaysReadyProbe();
        swarmClient = new FileSwarmClient(Path.Combine(dataDir, "objects"));
    }

    var instrumentedClient = new InstrumentedSwarmClient(swarmClient, monitoringMetrics);
    services.AddSwarmKeyDbStore(_ => new SwarmKeyValueStore(instrumentedClient, index, integrityOptions));
}

using var provider = services.BuildServiceProvider();
cacheStats = provider.GetRequiredService<ICacheStats>();
var processor = new RedisCommandProcessor(
    provider.GetRequiredService<IKeyValueStore>(),
    provider.GetRequiredService<IEthAddressAccessor>(),
    monitoringMetrics,
    provider.GetRequiredService<ILogger<RedisCommandProcessor>>());
var server = new RedisServer(
    bind,
    port,
    processor,
    monitoringMetrics.OnConnectionOpened,
    monitoringMetrics.OnConnectionClosed,
    provider.GetRequiredService<ILogger<RedisServer>>());

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

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
        shardHealthProvider);
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
        shardHealthProvider);
    monitoringServers.Add(metricsServer);
    monitoringTasks.Add(metricsServer.RunAsync(cts.Token));
}

await server.RunAsync(cts.Token);
await Task.WhenAll(monitoringTasks);
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
