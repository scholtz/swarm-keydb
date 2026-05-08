using System.Text.Json.Serialization;

namespace SwarmKeyDb;

public sealed class AclEntry
{
    [JsonPropertyName("address")]
    public string EthAddress { get; set; } = string.Empty;

    [JsonPropertyName("permission")]
    public AclPermission Permission { get; set; }
}
