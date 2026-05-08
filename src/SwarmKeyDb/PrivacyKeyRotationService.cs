namespace SwarmKeyDb;

public sealed class PrivacyKeyRotationService
{
    private readonly PrivacyPreservingKeyIndex _index;

    public PrivacyKeyRotationService(PrivacyPreservingKeyIndex index)
    {
        _index = index;
    }

    public Task<int> RotateAsync(string newPrivacyKeyHex, bool dryRun = false, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(newPrivacyKeyHex))
        {
            throw new ArgumentException("newPrivacyKeyHex must be a non-empty hex string.", nameof(newPrivacyKeyHex));
        }

        var newStrategy = HmacSha256KeyStrategy.FromHexKey(newPrivacyKeyHex);
        return _index.RotateStrategyAsync(newStrategy, dryRun, cancellationToken);
    }
}
