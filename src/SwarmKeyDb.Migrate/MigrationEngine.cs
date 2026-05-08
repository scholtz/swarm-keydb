namespace SwarmKeyDb.Migrate;

public sealed class MigrationEngine
{
    private readonly IMigrationSource _source;
    private readonly IMigrationDestination _destination;
    private readonly IMigrationCheckpointStore _checkpointStore;
    private readonly IMigrationReporter _reporter;
    private readonly Random _random;

    public MigrationEngine(
        IMigrationSource source,
        IMigrationDestination destination,
        IMigrationCheckpointStore checkpointStore,
        IMigrationReporter reporter,
        Random? random = null)
    {
        _source = source;
        _destination = destination;
        _checkpointStore = checkpointStore;
        _reporter = reporter;
        _random = random ?? Random.Shared;
    }

    public async Task<MigrationResult> RunAsync(MigrationOptions options, CancellationToken cancellationToken)
    {
        var checkpoint = await _checkpointStore.LoadAsync(cancellationToken).ConfigureAwait(false);
        var estimatedTotal = await _source.GetApproximateTotalKeysAsync(cancellationToken).ConfigureAwait(false);

        var startedAt = DateTimeOffset.UtcNow;
        var counters = new MigrationCounters();
        var sampleKeys = new List<(string SourceKey, string DestinationKey)>();
        var matchPattern = BuildScanPattern(options.Prefix);

        if (checkpoint.PendingBatchKeys.Count > 0 && checkpoint.PendingBatchNextCursor is not null)
        {
            var resumed = await ProcessBatchAsync(
                checkpoint.PendingBatchKeys,
                checkpoint.PendingBatchIndex,
                checkpoint.PendingBatchNextCursor.Value,
                options,
                counters,
                sampleKeys,
                estimatedTotal,
                startedAt,
                cancellationToken).ConfigureAwait(false);
            checkpoint = resumed;
        }

        var cursor = checkpoint.Cursor;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var batch = await _source.ScanAsync(cursor, matchPattern, options.ScanCount, cancellationToken).ConfigureAwait(false);
            var nextCheckpoint = new MigrationCheckpoint
            {
                Cursor = cursor,
                PendingBatchNextCursor = batch.NextCursor,
                PendingBatchKeys = [.. batch.Keys],
                PendingBatchIndex = 0
            };
            await _checkpointStore.SaveAsync(nextCheckpoint, cancellationToken).ConfigureAwait(false);

            checkpoint = await ProcessBatchAsync(
                batch.Keys,
                0,
                batch.NextCursor,
                options,
                counters,
                sampleKeys,
                estimatedTotal,
                startedAt,
                cancellationToken).ConfigureAwait(false);

            cursor = checkpoint.Cursor;
        }
        while (cursor != 0);

        var validationMismatches = options.Validate
            ? await ValidateSampleAsync(sampleKeys, options.ValidateSamplePercent, cancellationToken).ConfigureAwait(false)
            : [];

        await _checkpointStore.DeleteAsync(cancellationToken).ConfigureAwait(false);

        var finalProgress = BuildProgress(counters.Scanned, counters.Migrated, counters.Errors, estimatedTotal, startedAt);
        var result = new MigrationResult
        {
            Progress = finalProgress,
            ValidationMismatches = validationMismatches
        };
        _reporter.ReportSummary(result);
        return result;
    }

    public static string BuildScanPattern(string? prefix) =>
        string.IsNullOrWhiteSpace(prefix) ? "*" : $"{prefix}*";

    private async Task<MigrationCheckpoint> ProcessBatchAsync(
        IReadOnlyList<string> keys,
        int startIndex,
        ulong nextCursor,
        MigrationOptions options,
        MigrationCounters counters,
        List<(string SourceKey, string DestinationKey)> sampleKeys,
        long? estimatedTotal,
        DateTimeOffset startedAt,
        CancellationToken cancellationToken)
    {
        var checkpoint = new MigrationCheckpoint
        {
            Cursor = 0,
            PendingBatchNextCursor = nextCursor,
            PendingBatchKeys = [.. keys],
            PendingBatchIndex = startIndex
        };

        for (var index = startIndex; index < keys.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            counters.Scanned++;
            var key = keys[index];
            try
            {
                var entry = await _source.ReadEntryAsync(key, cancellationToken).ConfigureAwait(false);
                if (entry is not null)
                {
                    var destinationKey = GetDestinationKey(entry.Key, options);
                    var destinationEntry = destinationKey == entry.Key
                        ? entry
                        : new MigrationEntry
                        {
                            Key = destinationKey,
                            Type = entry.Type,
                            Payload = entry.Payload,
                            Ttl = entry.Ttl
                        };
                    if (!options.DryRun)
                    {
                        await _destination.WriteEntryAsync(destinationEntry, cancellationToken).ConfigureAwait(false);
                    }

                    counters.Migrated++;
                    if (_random.Next(0, 100) < options.ValidateSamplePercent)
                    {
                        sampleKeys.Add((entry.Key, destinationKey));
                    }
                }
            }
            catch (Exception ex)
            {
                counters.Errors++;
                _reporter.ReportError(key, ex);
            }

            checkpoint = new MigrationCheckpoint
            {
                Cursor = 0,
                PendingBatchNextCursor = nextCursor,
                PendingBatchKeys = [.. keys],
                PendingBatchIndex = index + 1
            };
            await _checkpointStore.SaveAsync(checkpoint, cancellationToken).ConfigureAwait(false);

            _reporter.ReportProgress(BuildProgress(counters.Scanned, counters.Migrated, counters.Errors, estimatedTotal, startedAt));
        }

        var completed = new MigrationCheckpoint
        {
            Cursor = nextCursor,
            PendingBatchNextCursor = null,
            PendingBatchKeys = [],
            PendingBatchIndex = 0
        };
        await _checkpointStore.SaveAsync(completed, cancellationToken).ConfigureAwait(false);
        return completed;
    }

    private async Task<IReadOnlyList<ValidationMismatch>> ValidateSampleAsync(
        IReadOnlyList<(string SourceKey, string DestinationKey)> sampledKeys,
        int samplePercent,
        CancellationToken cancellationToken)
    {
        if (sampledKeys.Count == 0 || samplePercent <= 0)
        {
            return [];
        }

        var targetCount = Math.Max(1, sampledKeys.Count * samplePercent / 100);
        var keys = sampledKeys
            .OrderBy(_ => _random.Next())
            .Take(targetCount)
            .Distinct()
            .ToArray();

        var mismatches = new List<ValidationMismatch>();
        foreach (var keyPair in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sourceEntry = await _source.ReadEntryAsync(keyPair.SourceKey, cancellationToken).ConfigureAwait(false);
            var destinationValue = await _destination.ReadValueAsync(keyPair.DestinationKey, cancellationToken).ConfigureAwait(false);

            if (sourceEntry is null || destinationValue is null)
            {
                mismatches.Add(new ValidationMismatch { Key = keyPair.SourceKey, Reason = "missing source or destination value" });
                continue;
            }

            if (!sourceEntry.Payload.AsSpan().SequenceEqual(destinationValue.Payload))
            {
                mismatches.Add(new ValidationMismatch { Key = keyPair.SourceKey, Reason = "payload mismatch" });
                continue;
            }

            if (!TtlMatches(sourceEntry.Ttl, destinationValue.Ttl))
            {
                mismatches.Add(new ValidationMismatch
                {
                    Key = keyPair.SourceKey,
                    Reason = $"ttl mismatch source={sourceEntry.Ttl?.TotalSeconds:F0}s destination={destinationValue.Ttl?.TotalSeconds:F0}s"
                });
            }
        }

        return mismatches;
    }

    private static bool TtlMatches(TimeSpan? sourceTtl, TimeSpan? destinationTtl)
    {
        if (sourceTtl is null && destinationTtl is null)
        {
            return true;
        }

        if (sourceTtl is null || destinationTtl is null)
        {
            return false;
        }

        return Math.Abs((sourceTtl.Value - destinationTtl.Value).TotalSeconds) <= 1;
    }

    private static string GetDestinationKey(string sourceKey, MigrationOptions options)
    {
        if (!options.EnablePrivacy)
        {
            return sourceKey;
        }

        if (string.IsNullOrWhiteSpace(options.PrivacyKeyHex))
        {
            throw new InvalidOperationException("EnablePrivacy requires PrivacyKeyHex.");
        }

        return DeriveHmacToken(sourceKey, options.PrivacyKeyHex);
    }

    private static string DeriveHmacToken(string sourceKey, string keyHex)
    {
        var secret = Convert.FromHexString(keyHex);
        try
        {
            using var hmac = new System.Security.Cryptography.HMACSHA256(secret);
            return Convert.ToHexStringLower(hmac.ComputeHash(System.Text.Encoding.UTF8.GetBytes(sourceKey)));
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(secret);
        }
    }

    private static MigrationProgress BuildProgress(
        long scanned,
        long migrated,
        long errors,
        long? estimatedTotal,
        DateTimeOffset startedAt)
    {
        return new MigrationProgress
        {
            ScannedKeys = scanned,
            MigratedKeys = migrated,
            ErrorCount = errors,
            EstimatedTotalKeys = estimatedTotal,
            Elapsed = DateTimeOffset.UtcNow - startedAt
        };
    }

    private sealed class MigrationCounters
    {
        public long Scanned { get; set; }
        public long Migrated { get; set; }
        public long Errors { get; set; }
    }
}
