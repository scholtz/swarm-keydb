using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;

namespace SwarmKeyDb.SwarmConsistency;

public sealed class BeeConsistencyVerifier : ISwarmConsistencyVerifier
{
    private readonly HttpClient _httpClient;
    private readonly ConsistencyOptions _options;
    private readonly string _nodeUrl;

    public BeeConsistencyVerifier(HttpClient httpClient, ConsistencyOptions options)
    {
        _httpClient = httpClient;
        _options = options;
        _nodeUrl = _httpClient.BaseAddress?.ToString() ?? "unknown";
    }

    public async Task<VerificationResult> VerifyFeedRevisionAsync(string topic, ulong expectedRevision, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var (owner, parsedTopic) = ResolveOwnerAndTopic(topic);
        var timer = Stopwatch.StartNew();
        using var response = await _httpClient.GetAsync($"feeds/{owner}/{parsedTopic}", ct).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            timer.Stop();
            return VerificationResult.Failed("feed-revision", _nodeUrl, timer.Elapsed, expectedRevision.ToString(), $"http:{(int)response.StatusCode}", "Bee feed request failed.");
        }

        await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: ct).ConfigureAwait(false);
        var revision = ReadFeedRevision(document.RootElement);
        timer.Stop();
        if (revision is null)
        {
            return VerificationResult.Failed("feed-revision", _nodeUrl, timer.Elapsed, expectedRevision.ToString(), "<missing>", "Bee feed response did not contain feed revision/index.");
        }

        return revision.Value == expectedRevision
            ? VerificationResult.Passed("feed-revision", _nodeUrl, timer.Elapsed, expectedRevision.ToString(), revision.Value.ToString())
            : VerificationResult.Failed("feed-revision", _nodeUrl, timer.Elapsed, expectedRevision.ToString(), revision.Value.ToString(), "Feed revision mismatch.");
    }

    public async Task<VerificationResult> VerifyContentHashAsync(string reference, byte[] expectedHash, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        ArgumentNullException.ThrowIfNull(expectedHash);
        var timer = Stopwatch.StartNew();
        using var response = await _httpClient.GetAsync($"bytes/{Uri.EscapeDataString(reference)}", ct).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            timer.Stop();
            return VerificationResult.Failed("content-hash", _nodeUrl, timer.Elapsed, Convert.ToHexStringLower(expectedHash), $"http:{(int)response.StatusCode}", "Bee bytes request failed.");
        }

        var payload = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
        var actualHash = SHA256.HashData(payload);
        timer.Stop();
        var expectedHex = Convert.ToHexStringLower(expectedHash);
        var actualHex = Convert.ToHexStringLower(actualHash);
        return actualHash.AsSpan().SequenceEqual(expectedHash)
            ? VerificationResult.Passed("content-hash", _nodeUrl, timer.Elapsed, expectedHex, actualHex)
            : VerificationResult.Failed("content-hash", _nodeUrl, timer.Elapsed, expectedHex, actualHex, "SHA-256 hash mismatch.");
    }

    public async Task<VerificationResult> VerifyManifestLineageAsync(string manifestRef, IReadOnlyList<string> expectedAncestors, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestRef);
        expectedAncestors ??= [];

        var timer = Stopwatch.StartNew();
        using var response = await _httpClient.GetAsync($"bzz/{Uri.EscapeDataString(manifestRef)}/?metadata=true", ct).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            timer.Stop();
            return VerificationResult.Failed("manifest-lineage", _nodeUrl, timer.Elapsed, string.Join(",", expectedAncestors), $"http:{(int)response.StatusCode}", "Bee manifest request failed.");
        }

        var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        timer.Stop();
        var missing = expectedAncestors.Where(ancestor => !body.Contains(ancestor, StringComparison.OrdinalIgnoreCase)).ToArray();
        return missing.Length == 0
            ? VerificationResult.Passed("manifest-lineage", _nodeUrl, timer.Elapsed, string.Join(",", expectedAncestors), "all-ancestors-present")
            : VerificationResult.Failed("manifest-lineage", _nodeUrl, timer.Elapsed, string.Join(",", expectedAncestors), string.Join(",", missing), "Manifest lineage mismatch.");
    }

    private (string Owner, string Topic) ResolveOwnerAndTopic(string topic)
    {
        var parts = topic.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 2)
        {
            return (parts[0], parts[1]);
        }

        return (_options.FeedOwner, topic);
    }

    private static ulong? ReadFeedRevision(JsonElement root)
    {
        if (root.TryGetProperty("feedIndex", out var feedIndexElement))
        {
            return ParseRevision(feedIndexElement);
        }

        if (root.TryGetProperty("index", out var indexElement))
        {
            return ParseRevision(indexElement);
        }

        return null;
    }

    private static ulong? ParseRevision(JsonElement element) =>
        element.ValueKind switch
        {
            JsonValueKind.Number when element.TryGetUInt64(out var numeric) => numeric,
            JsonValueKind.String => ParseRevision(element.GetString()),
            _ => null
        };

    private static ulong? ParseRevision(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var normalized = raw.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? raw[2..] : raw;
        if (ulong.TryParse(normalized, System.Globalization.NumberStyles.HexNumber, null, out var hex))
        {
            return hex;
        }

        return ulong.TryParse(normalized, out var numeric) ? numeric : null;
    }
}
