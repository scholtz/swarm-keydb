using System.Text;
using System.Text.Json;
using SwarmKeyDb;

namespace SwarmKeyDb.Cli;

public static class SwarmKeyDbCliApp
{
    public static Task<int> RunAsync(
        string[] args,
        TextWriter stdout,
        TextWriter stderr,
        CliExecutionOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var runtime = new CliRuntime(stdout, stderr, options ?? CliExecutionOptions.Default);
        return runtime.RunAsync(args, cancellationToken);
    }
}

public sealed class CliExecutionOptions
{
    public static CliExecutionOptions Default { get; } = new();

    public Func<RuntimeSettings, ISwarmClient> SwarmClientFactory { get; init; } =
        static settings => new BeeSwarmClient(new Uri(settings.BeeUrl), settings.BatchId);

    public Func<string, IKeyIndex> KeyIndexFactory { get; init; } = static indexPath => new FileKeyIndex(indexPath);

    public Func<EnvironmentSnapshot> EnvironmentFactory { get; init; } = static () => EnvironmentSnapshot.Read();
}

public sealed record RuntimeSettings(
    string BeeUrl,
    string BatchId,
    OutputFormat Output,
    string ConfigPath,
    string IndexPath,
    bool BeeUrlFromEnvironment,
    bool BatchIdFromEnvironment);

public enum OutputFormat
{
    Plain,
    Json,
    Table
}

public sealed class EnvironmentSnapshot
{
    public string? Home { get; init; }
    public string? UserProfile { get; init; }
    public string? BeeUrl { get; init; }
    public string? BatchId { get; init; }
    public string? Output { get; init; }

    public static EnvironmentSnapshot Read() => new()
    {
        Home = Environment.GetEnvironmentVariable("HOME"),
        UserProfile = Environment.GetEnvironmentVariable("USERPROFILE"),
        BeeUrl = Environment.GetEnvironmentVariable("SWARMKEYDB_BEE_URL"),
        BatchId = Environment.GetEnvironmentVariable("SWARMKEYDB_BATCH_ID"),
        Output = Environment.GetEnvironmentVariable("SWARMKEYDB_OUTPUT")
    };
}

internal sealed class CliRuntime
{
    private const string DefaultBeeUrl = "http://localhost:1633/";
    private readonly TextWriter _stdout;
    private readonly TextWriter _stderr;
    private readonly CliExecutionOptions _options;

    public CliRuntime(TextWriter stdout, TextWriter stderr, CliExecutionOptions options)
    {
        _stdout = stdout;
        _stderr = stderr;
        _options = options;
    }

    public async Task<int> RunAsync(string[] args, CancellationToken cancellationToken)
    {
        try
        {
            var parsed = ParseArgs(args);
            if (parsed.ShowHelp)
            {
                WriteHelp(parsed.Command);
                return 0;
            }

            if (parsed.Command is null)
            {
                WriteHelp(null);
                return 0;
            }

            var env = _options.EnvironmentFactory();
            if (parsed.Command.Equals("config", StringComparison.OrdinalIgnoreCase))
            {
                return await ExecuteConfigAsync(parsed, env, cancellationToken).ConfigureAwait(false);
            }

            var settings = ResolveSettings(parsed, env);
            await using var context = CreateContext(settings);
            return await ExecuteDataCommandAsync(parsed, context, cancellationToken).ConfigureAwait(false);
        }
        catch (CliUsageException ex)
        {
            await _stderr.WriteLineAsync(ex.Message).ConfigureAwait(false);
            return 1;
        }
        catch (CliExecutionException ex)
        {
            await _stderr.WriteLineAsync(ex.Message).ConfigureAwait(false);
            return 1;
        }
        catch (HttpRequestException)
        {
            await _stderr.WriteLineAsync("Bee node unreachable — check --bee-url or SWARMKEYDB_BEE_URL and verify the node is running.").ConfigureAwait(false);
            return 1;
        }
        catch (Exception ex)
        {
            await _stderr.WriteLineAsync($"Command failed: {ex.Message}").ConfigureAwait(false);
            return 1;
        }
    }

    private async Task<int> ExecuteConfigAsync(ParsedArgs parsed, EnvironmentSnapshot env, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var configPath = GetConfigPath(env);
        var config = await CliConfigStore.LoadAsync(configPath, cancellationToken).ConfigureAwait(false);

        var subcommand = parsed.Positionals.FirstOrDefault();
        if (subcommand is null)
        {
            throw new CliUsageException("Missing subcommand. Use: skdb config set|get");
        }

        if (subcommand.Equals("set", StringComparison.OrdinalIgnoreCase))
        {
            var beeUrl = parsed.TryGetOptionValue("--bee-url");
            var batchId = parsed.TryGetOptionValue("--batch-id");
            var outputText = parsed.TryGetOptionValue("--output");
            if (beeUrl is null && batchId is null && outputText is null)
            {
                throw new CliUsageException("No values provided. Use --bee-url, --batch-id, or --output.");
            }

            if (beeUrl is not null)
            {
                ValidateUri(beeUrl);
                config.BeeUrl = beeUrl;
            }

            if (batchId is not null)
            {
                config.BatchId = batchId;
            }

            if (outputText is not null)
            {
                config.Output = ParseOutput(outputText).ToString().ToLowerInvariant();
            }

            await CliConfigStore.SaveAsync(configPath, config, cancellationToken).ConfigureAwait(false);
            await WriteOutputAsync(ParseOutput(parsed.TryGetOptionValue("--output") ?? config.Output ?? "plain"),
                new { saved = true, configPath, config.BeeUrl, config.BatchId, config.Output },
                $"Saved config at {configPath}").ConfigureAwait(false);
            return 0;
        }

        if (subcommand.Equals("get", StringComparison.OrdinalIgnoreCase))
        {
            var key = parsed.Positionals.Skip(1).FirstOrDefault();
            var output = ParseOutput(parsed.TryGetOptionValue("--output") ?? config.Output ?? "plain");
            if (string.IsNullOrWhiteSpace(key))
            {
                await WriteOutputAsync(output,
                    new { config.BeeUrl, config.BatchId, config.Output, configPath },
                    $"bee-url={config.BeeUrl ?? string.Empty}\nbatch-id={config.BatchId ?? string.Empty}\noutput={config.Output ?? "plain"}\nconfig={configPath}").ConfigureAwait(false);
                return 0;
            }

            var value = key switch
            {
                "bee-url" => config.BeeUrl,
                "batch-id" => config.BatchId,
                "output" => config.Output,
                _ => throw new CliUsageException("Unsupported key. Use bee-url, batch-id, or output.")
            };

            await WriteOutputAsync(output, new { key, value }, value ?? string.Empty).ConfigureAwait(false);
            return 0;
        }

        throw new CliUsageException("Unknown config subcommand. Use: skdb config set|get");
    }

    private async Task<int> ExecuteDataCommandAsync(ParsedArgs parsed, DataContext context, CancellationToken cancellationToken)
    {
        switch (parsed.Command)
        {
            case "put":
            {
                var key = parsed.Positionals.FirstOrDefault() ?? throw new CliUsageException("put requires <key>.");
                var inlineValue = parsed.Positionals.Skip(1).FirstOrDefault();
                var filePath = parsed.TryGetOptionValue("--file");
                if (inlineValue is null && filePath is null)
                {
                    throw new CliUsageException("put requires <value> or --file <path>.");
                }

                if (inlineValue is not null && filePath is not null)
                {
                    throw new CliUsageException("Use either inline <value> or --file, not both.");
                }

                var bytes = filePath is not null
                    ? await File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false)
                    : Encoding.UTF8.GetBytes(inlineValue!);

                await context.Client.PutBytesAsync(key, bytes, cancellationToken).ConfigureAwait(false);
                await WriteOutputAsync(context.Settings.Output, new { key, size = bytes.Length, stored = true }, $"OK {key}").ConfigureAwait(false);
                return 0;
            }
            case "get":
            {
                var key = parsed.Positionals.FirstOrDefault() ?? throw new CliUsageException("get requires <key>.");
                var value = await context.Client.GetBytesAsync(key, cancellationToken).ConfigureAwait(false);
                var utf8Value = value is null ? null : Encoding.UTF8.GetString(value);
                await WriteOutputAsync(context.Settings.Output,
                    new { key, found = value is not null, value = utf8Value, valueBase64 = value is null ? null : Convert.ToBase64String(value) },
                    value is null ? string.Empty : utf8Value ?? string.Empty).ConfigureAwait(false);
                return value is null ? 1 : 0;
            }
            case "delete":
            {
                var key = parsed.Positionals.FirstOrDefault() ?? throw new CliUsageException("delete requires <key>.");
                var deleted = await context.Client.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
                await WriteOutputAsync(context.Settings.Output, new { key, deleted }, deleted ? "1" : "0").ConfigureAwait(false);
                return deleted ? 0 : 1;
            }
            case "list":
            {
                var prefix = parsed.TryGetOptionValue("--prefix");
                var keys = prefix is null
                    ? await context.Client.KeysAsync(cancellationToken).ConfigureAwait(false)
                    : await context.Client.GetKeysWithPrefixAsync(prefix, cancellationToken).ConfigureAwait(false);
                await WriteOutputAsync(context.Settings.Output, new { count = keys.Count, keys }, string.Join(Environment.NewLine, keys)).ConfigureAwait(false);
                return 0;
            }
            case "scan":
            {
                var from = parsed.TryGetOptionValue("--from") ?? throw new CliUsageException("scan requires --from <start>.");
                var to = parsed.TryGetOptionValue("--to") ?? throw new CliUsageException("scan requires --to <end>.");
                var items = await context.Client.GetKeyRangeAsync(from, to, new RangeScanOptions { IncludeValues = false }, cancellationToken).ConfigureAwait(false);
                var keys = items.Select(static item => item.Key).ToArray();
                await WriteOutputAsync(context.Settings.Output, new { from, to, count = keys.Length, keys }, string.Join(Environment.NewLine, keys)).ConfigureAwait(false);
                return 0;
            }
            case "stats":
            {
                var keys = await context.Client.KeysAsync(cancellationToken).ConfigureAwait(false);
                long totalBytes = 0;
                foreach (var key in keys)
                {
                    var value = await context.Client.GetBytesAsync(key, cancellationToken).ConfigureAwait(false);
                    totalBytes += value?.Length ?? 0;
                }

                await WriteOutputAsync(context.Settings.Output,
                    new
                    {
                        keyCount = keys.Count,
                        storageBytes = totalBytes,
                        beeUrl = context.Settings.BeeUrl,
                        batchId = context.Settings.BatchId,
                        indexPath = context.Settings.IndexPath
                    },
                    $"keys={keys.Count}\nstorage-bytes={totalBytes}\nbee-url={context.Settings.BeeUrl}\nbatch-id={context.Settings.BatchId}\nindex={context.Settings.IndexPath}").ConfigureAwait(false);
                return 0;
            }
            default:
                throw new CliUsageException($"Unknown command '{parsed.Command}'. Use --help.");
        }
    }

    private DataContext CreateContext(RuntimeSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.BatchId))
        {
            throw new CliUsageException("Missing Bee postage batch id. Set with 'skdb config set --batch-id <id>' or SWARMKEYDB_BATCH_ID.");
        }

        try
        {
            var swarm = _options.SwarmClientFactory(settings);
            var index = _options.KeyIndexFactory(settings.IndexPath);
            var store = new SwarmKeyValueStore(swarm, index);
            return new DataContext(new SwarmKeyDbClient(store), settings, swarm as IDisposable);
        }
        catch (UriFormatException)
        {
            throw new CliUsageException($"Invalid Bee URL '{settings.BeeUrl}'.");
        }
    }

    private static RuntimeSettings ResolveSettings(ParsedArgs parsed, EnvironmentSnapshot env)
    {
        var configPath = GetConfigPath(env);
        var config = CliConfigStore.Load(configPath);

        var beeFlag = parsed.TryGetOptionValue("--bee-url");
        if (beeFlag is not null)
        {
            ValidateUri(beeFlag);
        }

        var beeUrl = beeFlag ?? env.BeeUrl ?? config.BeeUrl ?? DefaultBeeUrl;
        var batchId = parsed.TryGetOptionValue("--batch-id") ?? env.BatchId ?? config.BatchId ?? string.Empty;
        var output = ParseOutput(parsed.TryGetOptionValue("--output") ?? env.Output ?? config.Output ?? "plain");

        var configDir = Path.GetDirectoryName(configPath)
            ?? throw new InvalidOperationException("Could not determine configuration directory.");
        var indexPath = Path.Combine(configDir, "index.json");

        return new RuntimeSettings(
            BeeUrl: beeUrl,
            BatchId: batchId,
            Output: output,
            ConfigPath: configPath,
            IndexPath: indexPath,
            BeeUrlFromEnvironment: env.BeeUrl is not null,
            BatchIdFromEnvironment: env.BatchId is not null);
    }

    private static ParsedArgs ParseArgs(string[] args)
    {
        var positionals = new List<string>();
        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var showHelp = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg is "--help" or "-h")
            {
                showHelp = true;
                continue;
            }

            if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                if (i + 1 >= args.Length)
                {
                    throw new CliUsageException($"Missing value for option {arg}.");
                }

                options[arg] = args[++i];
                continue;
            }

            positionals.Add(arg);
        }

        var command = positionals.FirstOrDefault();
        var commandArgs = command is null ? Array.Empty<string>() : positionals.Skip(1).ToArray();
        return new ParsedArgs(command, commandArgs, options, showHelp);
    }

    private void WriteHelp(string? command)
    {
        var help = command switch
        {
            null =>
                """
                Usage: skdb [global-options] <command> [arguments]

                Commands:
                  get <key>
                  put <key> <value> [--file <path>]
                  delete <key>
                  list [--prefix <prefix>]
                  scan --from <start> --to <end>
                  stats
                  config set [--bee-url <url>] [--batch-id <id>] [--output plain|json|table]
                  config get [bee-url|batch-id|output]

                Global options:
                  --bee-url <url>
                  --batch-id <id>
                  --output plain|json|table
                  --help

                Environment overrides:
                  SWARMKEYDB_BEE_URL
                  SWARMKEYDB_BATCH_ID
                  SWARMKEYDB_OUTPUT
                """,
            "put" => "Usage: skdb put <key> <value> | skdb put <key> --file <path>",
            "get" => "Usage: skdb get <key>",
            "delete" => "Usage: skdb delete <key>",
            "list" => "Usage: skdb list [--prefix <prefix>]",
            "scan" => "Usage: skdb scan --from <start> --to <end>",
            "stats" => "Usage: skdb stats",
            "config" => "Usage: skdb config set|get ...",
            _ => "Usage: skdb --help"
        };

        _stdout.WriteLine(help);
    }

    private async Task WriteOutputAsync(OutputFormat format, object payload, string plainText)
    {
        switch (format)
        {
            case OutputFormat.Json:
                await _stdout.WriteLineAsync(JsonSerializer.Serialize(payload)).ConfigureAwait(false);
                break;
            case OutputFormat.Table:
                await _stdout.WriteLineAsync(ToTable(payload)).ConfigureAwait(false);
                break;
            default:
                await _stdout.WriteLineAsync(plainText).ConfigureAwait(false);
                break;
        }
    }

    private static string ToTable(object payload)
    {
        var dictionary = payload.GetType().GetProperties()
            .ToDictionary(static p => p.Name, p => p.GetValue(payload), StringComparer.Ordinal);
        var sb = new StringBuilder();
        foreach (var (name, value) in dictionary)
        {
            if (value is IEnumerable<string> values && value is not string)
            {
                sb.Append(name).Append(':').Append(' ').Append(string.Join(",", values)).AppendLine();
                continue;
            }

            sb.Append(name).Append(':').Append(' ').Append(value?.ToString() ?? string.Empty).AppendLine();
        }

        return sb.ToString().TrimEnd();
    }

    private static string GetConfigPath(EnvironmentSnapshot env)
    {
        var home = env.Home;
        if (string.IsNullOrWhiteSpace(home))
        {
            home = env.UserProfile;
        }

        if (string.IsNullOrWhiteSpace(home))
        {
            home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        }

        if (string.IsNullOrWhiteSpace(home))
        {
            throw new CliExecutionException("Could not determine user home directory for ~/.swarmkeydb/config.json.");
        }

        return Path.Combine(home, ".swarmkeydb", "config.json");
    }

    private static OutputFormat ParseOutput(string value)
    {
        if (Enum.TryParse<OutputFormat>(value, ignoreCase: true, out var output))
        {
            return output;
        }

        throw new CliUsageException("Invalid output format. Use plain, json, or table.");
    }

    private static void ValidateUri(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out _))
        {
            throw new CliUsageException($"Invalid Bee URL '{value}'.");
        }
    }
}

public sealed class CliConfig
{
    public string? BeeUrl { get; set; }
    public string? BatchId { get; set; }
    public string? Output { get; set; }
}

public static class CliConfigStore
{
    public static CliConfig Load(string configPath)
    {
        if (!File.Exists(configPath))
        {
            return new CliConfig();
        }

        var json = File.ReadAllText(configPath);
        if (string.IsNullOrWhiteSpace(json))
        {
            return new CliConfig();
        }

        return JsonSerializer.Deserialize<CliConfig>(json) ?? new CliConfig();
    }

    public static async Task<CliConfig> LoadAsync(string configPath, CancellationToken cancellationToken)
    {
        if (!File.Exists(configPath))
        {
            return new CliConfig();
        }

        await using var stream = File.OpenRead(configPath);
        return await JsonSerializer.DeserializeAsync<CliConfig>(stream, cancellationToken: cancellationToken).ConfigureAwait(false)
               ?? new CliConfig();
    }

    public static async Task SaveAsync(string configPath, CliConfig config, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(configPath)
            ?? throw new InvalidOperationException("Could not determine config directory.");
        Directory.CreateDirectory(directory);
        await using var stream = File.Create(configPath);
        await JsonSerializer.SerializeAsync(stream, config, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}

internal sealed class ParsedArgs
{
    public ParsedArgs(string? command, IReadOnlyList<string> positionals, IReadOnlyDictionary<string, string> options, bool showHelp)
    {
        Command = command;
        Positionals = positionals;
        Options = options;
        ShowHelp = showHelp;
    }

    public string? Command { get; }
    public IReadOnlyList<string> Positionals { get; }
    public IReadOnlyDictionary<string, string> Options { get; }
    public bool ShowHelp { get; }

    public string? TryGetOptionValue(string name) =>
        Options.TryGetValue(name, out var value) ? value : null;
}

internal sealed class DataContext : IAsyncDisposable
{
    private readonly IDisposable? _disposable;

    public DataContext(SwarmKeyDbClient client, RuntimeSettings settings, IDisposable? disposable)
    {
        Client = client;
        Settings = settings;
        _disposable = disposable;
    }

    public SwarmKeyDbClient Client { get; }
    public RuntimeSettings Settings { get; }

    public ValueTask DisposeAsync()
    {
        _disposable?.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal sealed class CliUsageException : Exception
{
    public CliUsageException(string message) : base(message)
    {
    }
}

internal sealed class CliExecutionException : Exception
{
    public CliExecutionException(string message) : base(message)
    {
    }
}
