using System.Net;
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

var store = new SwarmKeyValueStore(swarmClient, index);
var processor = new RedisCommandProcessor(store);
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

static string RequireEnvironment(string name) =>
    Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException($"Environment variable {name} is required.");
