using Microsoft.Data.Sqlite;

namespace SwarmKeyDb;

public sealed class SqliteOfflineJournal : IOfflineJournal
{
    private readonly string _connectionString;

    public SqliteOfflineJournal(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var directory = Path.GetDirectoryName(path);
        if (string.IsNullOrWhiteSpace(directory))
        {
            throw new ArgumentException("SQLite offline journal path must include a directory.", nameof(path));
        }

        Directory.CreateDirectory(directory);
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        };
        _connectionString = builder.ToString();
        Initialize();
    }

    public async Task<long> AppendAsync(OfflineOperationType operationType, string key, byte[]? value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO offline_journal(created_at_utc, operation_type, key_name, value_blob)
            VALUES ($createdAtUtc, $operationType, $key, $value);
            SELECT last_insert_rowid();
            """;
        command.Parameters.AddWithValue("$createdAtUtc", DateTimeOffset.UtcNow.ToString("O"));
        command.Parameters.AddWithValue("$operationType", (int)operationType);
        command.Parameters.AddWithValue("$key", key);
        command.Parameters.AddWithValue("$value", value is null ? DBNull.Value : value);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    public async Task<IReadOnlyList<OfflineJournalEntry>> ReadBatchAsync(int limit, CancellationToken cancellationToken = default)
    {
        if (limit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), "limit must be greater than zero.");
        }

        var entries = new List<OfflineJournalEntry>(limit);
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT sequence_id, created_at_utc, operation_type, key_name, value_blob
            FROM offline_journal
            ORDER BY sequence_id ASC
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            entries.Add(new OfflineJournalEntry(
                reader.GetInt64(0),
                DateTimeOffset.Parse(reader.GetString(1), System.Globalization.CultureInfo.InvariantCulture),
                (OfflineOperationType)reader.GetInt32(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : (byte[])reader[4]));
        }

        return entries;
    }

    public async Task RemoveThroughAsync(long sequenceInclusive, CancellationToken cancellationToken = default)
    {
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM offline_journal WHERE sequence_id <= $sequence;";
        command.Parameters.AddWithValue("$sequence", sequenceInclusive);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM offline_journal;";
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    private SqliteConnection CreateConnection() => new(_connectionString);

    private void Initialize()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS offline_journal (
                sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                operation_type INTEGER NOT NULL,
                key_name TEXT NOT NULL,
                value_blob BLOB NULL
            );
            """;
        command.ExecuteNonQuery();
    }
}
