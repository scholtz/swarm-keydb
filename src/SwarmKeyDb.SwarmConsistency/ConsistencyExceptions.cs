namespace SwarmKeyDb.SwarmConsistency;

public sealed class ConsistencyViolationException : Exception
{
    public ConsistencyViolationException(string key, VerificationResult result)
        : base($"Consistency verification failed for key '{key}'. Expected='{result.ExpectedValue}', Actual='{result.ActualValue}', Node='{result.NodeUrl}', Reason='{result.FailureReason}'.")
    {
        Key = key;
        Result = result;
    }

    public string Key { get; }
    public VerificationResult Result { get; }
}

public sealed class QuorumNotMetException : Exception
{
    public QuorumNotMetException(string verificationType, int threshold, int succeeded, IReadOnlyList<VerificationResult> results)
        : base($"Quorum not met for {verificationType}. Required={threshold}, Succeeded={succeeded}.")
    {
        VerificationType = verificationType;
        Threshold = threshold;
        Succeeded = succeeded;
        Results = results;
    }

    public string VerificationType { get; }
    public int Threshold { get; }
    public int Succeeded { get; }
    public IReadOnlyList<VerificationResult> Results { get; }
}
