namespace SwarmKeyDb;

public interface IConsistencyVerificationStatusProvider
{
    ConsistencyVerificationSnapshot GetSnapshot();
}

public readonly record struct ConsistencyVerificationSnapshot(
    DateTimeOffset? LastVerificationUtc,
    long TotalVerifications,
    long ViolationCount,
    double SuccessRate,
    double WorstLatencyMs);

public sealed class NoOpConsistencyVerificationStatusProvider : IConsistencyVerificationStatusProvider
{
    public static readonly NoOpConsistencyVerificationStatusProvider Instance = new();

    public ConsistencyVerificationSnapshot GetSnapshot() =>
        new(
            LastVerificationUtc: null,
            TotalVerifications: 0,
            ViolationCount: 0,
            SuccessRate: 1D,
            WorstLatencyMs: 0D);
}
