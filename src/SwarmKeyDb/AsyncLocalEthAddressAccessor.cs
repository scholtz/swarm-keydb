using System.Threading;

namespace SwarmKeyDb;

public sealed class AsyncLocalEthAddressAccessor : IEthAddressAccessor
{
    private readonly AsyncLocal<string?> _currentAddress = new();

    public string? CurrentAddress
    {
        get => _currentAddress.Value;
        set => _currentAddress.Value = value;
    }
}
