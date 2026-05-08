using System.Net;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SwarmKeyDb;
using SwarmKeyDb.Server;

var port = GetInt("SWARM_KEYDB_PORT", 6379);
var bind = IPAddress.Parse(Environment.GetEnvironmentVariable("SWARM_KEYDB_BIND") ?? "0.0.0.0");
var dataDir = Environment.GetEnvironmentVariable("SWARM_KEYDB_DATA_DIR") ?? Path.Combine(AppContext.BaseDirectory, "data");
var backend = Environment.GetEnvironmentVariable("SWARM_KEYDB_BACKEND") ?? "local";
var index = new FileKeyIndex(Path.Combine(dataDir, "index.json"));
ISwarmClient swarmClient = backend.Equals("bee", StringComparison.OrdinalIgnoreCase)
    ? new BeeSwarmClient(new Uri(Environment.GetEnvironmentVariable("BEE_URL") ?? "http://localhost:1633/"), RequireEnvironment("BEE_POSTAGE_BATCH_ID"))
    : new FileSwarmClient(Path.Combine(dataDir, "objects"));
var cacheOptions = new CacheOptions
{
    Enabled = GetBool("SWARM_KEYDB_CACHE_ENABLED", true),
    MaxEntries = Math.Max(CacheOptions.MinimumMaxEntries, GetInt("SWARM_KEYDB_CACHE_MAX_ENTRIES", 1_000)),
    DefaultEntryTtl = GetNullableInt("SWARM_KEYDB_CACHE_DEFAULT_TTL_SECONDS") is { } ttlSeconds
        ? TimeSpan.FromSeconds(ttlSeconds)
        : null
};
var services = new ServiceCollection();
services.AddLogging(builder => builder.AddSimpleConsole().SetMinimumLevel(GetLogLevel("SWARM_KEYDB_LOG_LEVEL", LogLevel.Information)));
services.AddOptions();
services.AddSingleton<IOptions<CacheOptions>>(Options.Create(cacheOptions));
services.AddSingleton<IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
services.AddSingleton<IKeyValueStore>(sp => new CachingKeyValueStore(
    new SwarmKeyValueStore(swarmClient, index),
    sp.GetRequiredService<IMemoryCache>(),
    sp.GetRequiredService<IOptions<CacheOptions>>(),
    sp.GetRequiredService<ILogger<CachingKeyValueStore>>()));
services.AddSingleton<ICacheStats>(sp => (ICacheStats)sp.GetRequiredService<IKeyValueStore>());

using var provider = services.BuildServiceProvider();
var processor = new RedisCommandProcessor(provider.GetRequiredService<IKeyValueStore>());
var server = new RedisServer(bind, port, processor);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

await server.RunAsync(cts.Token);

static int GetInt(string name, int defaultValue) =>
    int.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : defaultValue;

static int? GetNullableInt(string name) =>
    int.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : null;

static bool GetBool(string name, bool defaultValue) =>
    bool.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : defaultValue;

static LogLevel GetLogLevel(string name, LogLevel defaultValue) =>
    Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable(name), ignoreCase: true, out var level) ? level : defaultValue;

static string RequireEnvironment(string name) =>
    Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException($"Environment variable {name} is required.");
