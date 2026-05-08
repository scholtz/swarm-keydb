using System.Text;
using SwarmKeyDb;

var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
var db = new SwarmKeyDbClient(store);

await db.BatchPutAsync(new[]
{
    new KeyValuePair<string, ReadOnlyMemory<byte>>("config:featureA", Encoding.UTF8.GetBytes("enabled")),
    new KeyValuePair<string, ReadOnlyMemory<byte>>("config:featureB", Encoding.UTF8.GetBytes("disabled")),
    new KeyValuePair<string, ReadOnlyMemory<byte>>("config:maxUsers", Encoding.UTF8.GetBytes("500"))
});

var values = await db.BatchGetAsync(new[] { "config:featureA", "config:featureB", "config:maxUsers" });
for (var i = 0; i < values.Count; i++)
{
    Console.WriteLine($"config[{i}] = {Encoding.UTF8.GetString(values[i] ?? [])}");
}
