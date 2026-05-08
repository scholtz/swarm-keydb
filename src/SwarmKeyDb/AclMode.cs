using System.Text.Json.Serialization;

namespace SwarmKeyDb;

[JsonConverter(typeof(JsonStringEnumConverter<AclMode>))]
public enum AclMode
{
    Allowlist,
    Denylist
}
