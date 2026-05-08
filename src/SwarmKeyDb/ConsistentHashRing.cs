using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace SwarmKeyDb;

public sealed class ConsistentHashRing
{
    private readonly ulong[] _positions;
    private readonly string[] _owners;

    public ConsistentHashRing(IEnumerable<string> nodes, int virtualNodesPerNode)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        if (virtualNodesPerNode <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(virtualNodesPerNode), "virtualNodesPerNode must be greater than zero.");
        }

        var owners = nodes.Where(static node => !string.IsNullOrWhiteSpace(node)).Distinct(StringComparer.Ordinal).ToArray();
        if (owners.Length == 0)
        {
            throw new ArgumentException("At least one node is required to build a consistent hash ring.", nameof(nodes));
        }

        var points = new List<(ulong Position, string Owner)>(owners.Length * virtualNodesPerNode);
        foreach (var owner in owners)
        {
            for (var replica = 0; replica < virtualNodesPerNode; replica++)
            {
                points.Add((HashToUInt64($"{owner}#{replica}"), owner));
            }
        }

        points.Sort(static (left, right) => left.Position.CompareTo(right.Position));
        _positions = points.Select(static point => point.Position).ToArray();
        _owners = points.Select(static point => point.Owner).ToArray();
    }

    public string GetNode(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        var position = HashToUInt64(key);
        var index = Array.BinarySearch(_positions, position);
        if (index < 0)
        {
            index = ~index;
        }

        if (index >= _positions.Length)
        {
            index = 0;
        }

        return _owners[index];
    }

    internal static ulong HashToUInt64(string value)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return BinaryPrimitives.ReadUInt64BigEndian(hash);
    }
}
