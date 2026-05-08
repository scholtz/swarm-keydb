using System.Text.Json.Serialization;

namespace SwarmKeyDb;

[JsonConverter(typeof(JsonStringEnumConverter<AclPermission>))]
public enum AclPermission
{
    Read,
    Write,
    Admin
}
