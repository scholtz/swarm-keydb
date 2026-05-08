namespace SwarmKeyDb;

internal static class QueryScanHelpers
{
    public static bool MatchesLowerBound(string key, string? startKey, bool includeStart)
    {
        if (startKey is null)
        {
            return true;
        }

        var comparison = StringComparer.Ordinal.Compare(key, startKey);
        return includeStart ? comparison >= 0 : comparison > 0;
    }

    public static bool MatchesUpperBound(string key, string? endKey, bool includeEnd)
    {
        if (endKey is null)
        {
            return true;
        }

        var comparison = StringComparer.Ordinal.Compare(key, endKey);
        return includeEnd ? comparison <= 0 : comparison < 0;
    }

    public static int DecodeCursor(string? cursor, int upperBound)
    {
        if (string.IsNullOrEmpty(cursor))
        {
            return 0;
        }

        try
        {
            var raw = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(cursor));
            if (!int.TryParse(raw, out var offset) || offset < 0 || offset > upperBound)
            {
                throw new ArgumentException("cursor is invalid.", nameof(cursor));
            }

            return offset;
        }
        catch (FormatException)
        {
            throw new ArgumentException("cursor is invalid.", nameof(cursor));
        }
    }

    public static string EncodeCursor(int value) =>
        Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
}
