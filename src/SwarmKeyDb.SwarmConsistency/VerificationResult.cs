namespace SwarmKeyDb.SwarmConsistency;

public sealed record VerificationResult(
    bool IsValid,
    string VerificationType,
    string NodeUrl,
    TimeSpan Latency,
    string ExpectedValue,
    string ActualValue,
    string FailureReason)
{
    public static VerificationResult Passed(
        string verificationType,
        string nodeUrl,
        TimeSpan latency,
        string expectedValue,
        string actualValue) =>
        new(
            IsValid: true,
            VerificationType: verificationType,
            NodeUrl: nodeUrl,
            Latency: latency,
            ExpectedValue: expectedValue,
            ActualValue: actualValue,
            FailureReason: string.Empty);

    public static VerificationResult Failed(
        string verificationType,
        string nodeUrl,
        TimeSpan latency,
        string expectedValue,
        string actualValue,
        string failureReason) =>
        new(
            IsValid: false,
            VerificationType: verificationType,
            NodeUrl: nodeUrl,
            Latency: latency,
            ExpectedValue: expectedValue,
            ActualValue: actualValue,
            FailureReason: failureReason);
}
