using System.CommandLine;
using StackExchange.Redis;
using SwarmKeyDb.Migrate;

var fromOption = new Option<Uri>("--from")
{
    Description = "Source Redis URI (for example redis://localhost:6379)",
    Required = true
};
var toOption = new Option<Uri>("--to")
{
    Description = "Destination SwarmKeyDb Redis URI",
    Required = true
};
var dryRunOption = new Option<bool>("--dry-run")
{
    Description = "Read source keys and report migration progress without writing to destination."
};
var prefixOption = new Option<string?>("--prefix")
{
    Description = "Only migrate keys that start with this prefix."
};
var checkpointOption = new Option<FileInfo>("--checkpoint")
{
    Description = "Checkpoint file path used for resumable migration.",
    DefaultValueFactory = static _ => new FileInfo(Path.Combine(Environment.CurrentDirectory, ".swarmkeydb-migrate.checkpoint.json"))
};
var validateOption = new Option<bool>("--validate")
{
    Description = "Validate a sample of migrated keys after migration."
};
var validateSamplePercentOption = new Option<int>("--validate-sample-percent")
{
    Description = "Sample percentage for validation mode (1-100).",
    DefaultValueFactory = static _ => 5
};
var scanCountOption = new Option<int>("--scan-count")
{
    Description = "SCAN COUNT hint used when iterating source keys.",
    DefaultValueFactory = static _ => 500
};

var root = new RootCommand("Migrate Redis data into SwarmKeyDb over the Redis protocol.");
root.Add(fromOption);
root.Add(toOption);
root.Add(dryRunOption);
root.Add(prefixOption);
root.Add(checkpointOption);
root.Add(validateOption);
root.Add(validateSamplePercentOption);
root.Add(scanCountOption);

root.SetAction(async (parseResult, cancellationToken) =>
{
    var from = parseResult.GetRequiredValue(fromOption);
    var to = parseResult.GetRequiredValue(toOption);
    var dryRun = parseResult.GetValue(dryRunOption);
    var prefix = parseResult.GetValue(prefixOption);
    var checkpoint = parseResult.GetValue(checkpointOption) ?? new FileInfo(Path.Combine(Environment.CurrentDirectory, ".swarmkeydb-migrate.checkpoint.json"));
    var validate = parseResult.GetValue(validateOption);
    var validateSamplePercent = parseResult.GetValue(validateSamplePercentOption);
    var scanCount = parseResult.GetValue(scanCountOption);

    if (validateSamplePercent is < 1 or > 100)
    {
        Console.Error.WriteLine("--validate-sample-percent must be between 1 and 100.");
        return 1;
    }

    if (scanCount <= 0)
    {
        Console.Error.WriteLine("--scan-count must be greater than 0.");
        return 1;
    }

    using var sourceMux = await ConnectionMultiplexer.ConnectAsync(from.ToString()).ConfigureAwait(false);
    using var destinationMux = await ConnectionMultiplexer.ConnectAsync(to.ToString()).ConfigureAwait(false);

    var engine = new MigrationEngine(
        new RedisMigrationSource(sourceMux.GetDatabase()),
        new RedisMigrationDestination(destinationMux.GetDatabase()),
        new FileMigrationCheckpointStore(checkpoint.FullName),
        new ConsoleMigrationReporter());

    var options = new MigrationOptions
    {
        SourceUri = from,
        DestinationUri = to,
        DryRun = dryRun,
        Prefix = prefix,
        CheckpointPath = checkpoint.FullName,
        Validate = validate,
        ValidateSamplePercent = validateSamplePercent,
        ScanCount = scanCount
    };

    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        cts.Cancel();
    };

    try
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token);
        var result = await engine.RunAsync(options, linkedCts.Token).ConfigureAwait(false);
        return result.ValidationMismatches.Count == 0 ? 0 : 2;
    }
    catch (OperationCanceledException)
    {
        Console.Error.WriteLine("Migration canceled.");
        return 130;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Migration failed: {ex.Message}");
        return 1;
    }
});

return await root.Parse(args).InvokeAsync().ConfigureAwait(false);
