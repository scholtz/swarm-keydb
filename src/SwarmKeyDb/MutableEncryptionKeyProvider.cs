namespace SwarmKeyDb;

public sealed class MutableEncryptionKeyProvider : IEncryptionKeyProvider
{
    private readonly object _gate = new();
    private EncryptionOptions _options;

    public MutableEncryptionKeyProvider(EncryptionOptions options)
    {
        _options = Clone(options);
    }

    public EncryptionOptions GetOptions()
    {
        lock (_gate)
        {
            return Clone(_options);
        }
    }

    public byte[]? GetCurrentKey()
    {
        lock (_gate)
        {
            return EncryptionKeyMaterial.TryResolveKey(_options);
        }
    }

    public void Update(EncryptionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        lock (_gate)
        {
            _options = Clone(options);
        }
    }

    private static EncryptionOptions Clone(EncryptionOptions options) => new()
    {
        Enabled = options.Enabled,
        Algorithm = options.Algorithm,
        KeyHex = options.KeyHex,
        EthPrivateKeyHex = options.EthPrivateKeyHex
    };
}
