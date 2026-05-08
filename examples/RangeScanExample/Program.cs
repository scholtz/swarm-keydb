using System.Text;
using SwarmKeyDb;

var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
var client = new SwarmKeyDbClient(store);

await client.PutStringAsync("chat:0001", "hello");
await client.PutStringAsync("chat:0002", "how are you?");
await client.PutStringAsync("chat:0003", "bye");
await client.PutStringAsync("profile:alice", "active");

Console.WriteLine("Prefix scan (chat:):");
foreach (var key in await client.GetKeysWithPrefixAsync("chat:"))
{
    Console.WriteLine($"- {key}");
}

Console.WriteLine();
Console.WriteLine("Range scan (chat:0001..chat:0002, descending, include values):");
var range = await client.GetKeyRangeAsync("chat:0001", "chat:0002", new RangeScanOptions
{
    Descending = true,
    IncludeValues = true
});
foreach (var entry in range)
{
    Console.WriteLine($"- {entry.Key}: {Encoding.UTF8.GetString(entry.Value ?? [])}");
}

Console.WriteLine();
Console.WriteLine("Paginated scan (count=2):");
var page = await client.ScanAsync(null, 2);
while (true)
{
    Console.WriteLine($"cursor={page.NextCursor}");
    foreach (var key in page.Keys)
    {
        Console.WriteLine($"- {key}");
    }

    if (string.IsNullOrEmpty(page.NextCursor))
    {
        break;
    }

    page = await client.ScanAsync(page.NextCursor, 2);
}
