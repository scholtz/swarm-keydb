using System.Text.Json;

namespace SwarmKeyDb.Migrate;

public sealed class FileMigrationCheckpointStore : IMigrationCheckpointStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true
    };

    private readonly string _path;

    public FileMigrationCheckpointStore(string path)
    {
        _path = path;
    }

    public async Task<MigrationCheckpoint> LoadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_path))
        {
            return MigrationCheckpoint.Start;
        }

        await using var stream = File.OpenRead(_path);
        var checkpoint = await JsonSerializer.DeserializeAsync<MigrationCheckpoint>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
        return checkpoint ?? MigrationCheckpoint.Start;
    }

    public async Task SaveAsync(MigrationCheckpoint checkpoint, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var tempPath = _path + ".tmp";
        await using (var stream = File.Create(tempPath))
        {
            await JsonSerializer.SerializeAsync(stream, checkpoint, JsonOptions, cancellationToken).ConfigureAwait(false);
        }

        File.Move(tempPath, _path, overwrite: true);
    }

    public Task DeleteAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (File.Exists(_path))
        {
            File.Delete(_path);
        }

        return Task.CompletedTask;
    }
}

public sealed class ConsoleMigrationReporter : IMigrationReporter
{
    private DateTimeOffset _lastProgress = DateTimeOffset.MinValue;

    public void ReportProgress(MigrationProgress progress)
    {
        var now = DateTimeOffset.UtcNow;
        if (now - _lastProgress < TimeSpan.FromSeconds(1) && progress.EstimatedRemaining is not null)
        {
            return;
        }

        _lastProgress = now;
        var total = progress.EstimatedTotalKeys?.ToString() ?? "?";
        var percentage = progress.EstimatedTotalKeys is > 0
            ? $" ({progress.MigratedKeys * 100.0 / progress.EstimatedTotalKeys.Value:F1}%)"
            : string.Empty;
        var eta = progress.EstimatedRemaining?.ToString("c") ?? "n/a";

        Console.WriteLine($"Migrating... {progress.MigratedKeys} / {total} keys{percentage} | {progress.KeysPerSecond:F1} keys/sec | ETA: {eta} | errors: {progress.ErrorCount}");
    }

    public void ReportError(string key, Exception exception)
    {
        Console.Error.WriteLine($"[ERROR] key={key} type={exception.GetType().Name} message={exception.Message}");
    }

    public void ReportSummary(MigrationResult result)
    {
        Console.WriteLine($"Migration finished. Migrated={result.Progress.MigratedKeys}, scanned={result.Progress.ScannedKeys}, errors={result.Progress.ErrorCount}.");
        if (result.ValidationMismatches.Count == 0)
        {
            Console.WriteLine("Validation: OK");
            return;
        }

        Console.WriteLine($"Validation mismatches: {result.ValidationMismatches.Count}");
        foreach (var mismatch in result.ValidationMismatches.Take(20))
        {
            Console.WriteLine($" - {mismatch.Key}: {mismatch.Reason}");
        }
    }
}
