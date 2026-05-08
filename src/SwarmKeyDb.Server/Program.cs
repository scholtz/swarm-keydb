using System.Net;
using System.Text.Json;
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
var compressionOptions = new CompressionOptions
{
    Enabled = GetBool("SWARM_KEYDB_COMPRESSION_ENABLED", false),
    Algorithm = Enum.TryParse<CompressionAlgorithm>(
        Environment.GetEnvironmentVariable("SWARM_KEYDB_COMPRESSION_ALGORITHM"), ignoreCase: true, out var algo)
        ? algo
        : CompressionAlgorithm.GZip,
    MinSizeBytes = GetInt("SWARM_KEYDB_COMPRESSION_MIN_SIZE_BYTES", 64)
};
var encryptionOptions = new EncryptionOptions
{
    Enabled = GetBool("SWARM_KEYDB_ENCRYPTION_ENABLED", false),
    Algorithm = Enum.TryParse<EncryptionAlgorithm>(
        Environment.GetEnvironmentVariable("SWARM_KEYDB_ENCRYPTION_ALGORITHM"), ignoreCase: true, out var encAlgo)
        ? encAlgo
        : EncryptionAlgorithm.AesGcm256,
    KeyHex = Environment.GetEnvironmentVariable("SWARM_KEYDB_ENCRYPTION_KEY"),
    EthPrivateKeyHex = Environment.GetEnvironmentVariable("SWARM_KEYDB_ENCRYPTION_ETH_KEY")
};
var aclOptions = GetAclOptions();
var services = new ServiceCollection();
services.AddLogging(builder => builder.AddSimpleConsole().SetMinimumLevel(GetLogLevel("SWARM_KEYDB_LOG_LEVEL", LogLevel.Information)));
services.AddOptions();
services.AddSingleton<IOptions<CacheOptions>>(Options.Create(cacheOptions));
services.AddSingleton<IOptions<CompressionOptions>>(Options.Create(compressionOptions));
services.AddSingleton<IOptions<EncryptionOptions>>(Options.Create(encryptionOptions));
services.AddSingleton<IOptions<AclOptions>>(Options.Create(aclOptions));
services.AddSingleton<IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
services.AddSingleton<IEthAddressAccessor, AsyncLocalEthAddressAccessor>();
services.AddSwarmKeyDbStore(swarmClient, index);

using var provider = services.BuildServiceProvider();
var processor = new RedisCommandProcessor(
    provider.GetRequiredService<IKeyValueStore>(),
    provider.GetRequiredService<IEthAddressAccessor>());
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

static AclOptions GetAclOptions()
{
    var enabled = GetBool("SWARM_KEYDB_ACL_ENABLED", false);
    var modeText = Environment.GetEnvironmentVariable("SWARM_KEYDB_ACL_MODE");
    var mode = string.IsNullOrWhiteSpace(modeText)
        ? AclMode.Allowlist
        : Enum.TryParse<AclMode>(modeText, ignoreCase: true, out var parsedMode)
            ? parsedMode
            : throw new InvalidOperationException("SWARM_KEYDB_ACL_MODE must be 'allowlist' or 'denylist'.");

    var entriesJson = Environment.GetEnvironmentVariable("SWARM_KEYDB_ACL_ENTRIES");
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
