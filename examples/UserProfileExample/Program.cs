using SwarmKeyDb;

var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
var db = new SwarmKeyDbClient(store).WithNamespace("users:42:");

await db.PutJsonAsync("profile", new { name = "Ada", role = "admin", theme = "dark" });
var profile = await db.GetJsonAsync<Dictionary<string, object>>("profile");

Console.WriteLine($"Stored profile for user 42: {profile?["name"]} ({profile?["role"]})");
Console.WriteLine($"Namespace keys: {string.Join(", ", await db.KeysAsync())}");
