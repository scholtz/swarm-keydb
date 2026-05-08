using System.Text;

namespace SwarmKeyDb;

public static class KeyPrivacyStrategyFactory
{
    public static IKeyPrivacyStrategy Create(SwarmKeyDbOptions? options)
    {
        if (options is null || options.PrivacyMode == PrivacyMode.None)
        {
            return new PlaintextKeyStrategy();
        }

        if (string.IsNullOrWhiteSpace(options.PrivacyKeyHex))
        {
            throw new PrivacyModeException(
                "Privacy mode requires SwarmKeyDbOptions.PrivacyKeyHex so key tokens can be derived without revealing plaintext keys.");
        }

        return options.PrivacyMode switch
        {
            PrivacyMode.ObliviousHashing => HmacSha256KeyStrategy.FromHexKey(options.PrivacyKeyHex),
            PrivacyMode.FullPSI => new HmacSha256KeyStrategy(Encoding.UTF8.GetBytes(options.PrivacyKeyHex)),
            _ => new PlaintextKeyStrategy()
        };
    }
}
