using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace SwarmKeyDb;

public sealed class AsyncQueuedKeyValueStore : IKeyValueStore, IAsyncProcessingStore, ICacheStats, IBackendMetadataProvider, ICacheEviction
{
    private readonly IKeyValueStore _inner;
    private readonly AsyncProcessingOptions _options;
    private readonly ILogger<AsyncQueuedKeyValueStore> _logger;
    private readonly Channel<QueuedWriteOperation> _writeQueue;
    private readonly CancellationTokenSource _queueCancellation = new();
    private readonly object _flushLock = new();
    private TaskCompletionSource<bool>? _flushCompletionSource;
    private long _pendingWrites;

    public AsyncQueuedKeyValueStore(
        IKeyValueStore inner,
        AsyncProcessingOptions options,
        ILogger<AsyncQueuedKeyValueStore> logger)
    {
        _inner = inner;
        _options = options;
        _logger = logger;
        ValidateOptions(options);
        _writeQueue = Channel.CreateUnbounded<QueuedWriteOperation>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        _ = Task.Run(ProcessQueueAsync);
    }

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        QueueWriteAsync(async token =>
        {
            await _inner.PutAsync(key, value, token).ConfigureAwait(false);
            return true;
        }, cancellationToken);

    public Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default) =>
        QueueWriteAsync(async token =>
        {
            await _inner.PutWithStrategyAsync(key, value, mergeStrategy, token).ConfigureAwait(false);
            return true;
        }, cancellationToken);

    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        QueueWriteAsync(async token =>
        {
            await _inner.MergeAsync(key, incomingValue, token).ConfigureAwait(false);
            return true;
        }, cancellationToken);

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        QueueWriteAsync(async token =>
        {
            await _inner.SetKeyOptionsAsync(key, options, token).ConfigureAwait(false);
            return true;
        }, cancellationToken);

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        QueueWriteAsync(token => _inner.DeleteAsync(key, token), cancellationToken);

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        QueueWriteAsync(token => _inner.SetTtlAsync(key, ttl, token), cancellationToken);

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        QueueWriteAsync(token => _inner.RemoveTtlAsync(key, token), cancellationToken);

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetAsync(key, cancellationToken);

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _inner.ListKeysAsync(cancellationToken);

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(key, cancellationToken);

    public long Hits => (_inner as ICacheStats)?.Hits ?? 0;
    public long Misses => (_inner as ICacheStats)?.Misses ?? 0;
    public long Evictions => (_inner as ICacheStats)?.Evictions ?? 0;

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Task flushTask;
        lock (_flushLock)
        {
            if (Volatile.Read(ref _pendingWrites) == 0)
            {
                return Task.CompletedTask;
            }

            _flushCompletionSource ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            flushTask = _flushCompletionSource.Task;
        }

        return flushTask.WaitAsync(cancellationToken);
    }

    public void FireAndForget(Func<Task> operation, string operationName = "fire-and-forget")
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            var task = operation();
            if (task.IsCompleted)
            {
                if (task.IsFaulted)
                {
                    LogFireAndForgetFailure(task.Exception?.GetBaseException(), operationName);
                }

                return;
            }

            _ = task.ContinueWith(
                continuationTask =>
                {
                    LogFireAndForgetFailure(continuationTask.Exception?.GetBaseException(), operationName);
                },
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            LogFireAndForgetFailure(ex, operationName);
        }
    }

    public void FireAndForget(Action operation, string operationName = "fire-and-forget")
    {
        ArgumentNullException.ThrowIfNull(operation);
        FireAndForget(() =>
        {
            operation();
            return Task.CompletedTask;
        }, operationName);
    }

    private async Task<T> QueueWriteAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Interlocked.Increment(ref _pendingWrites);
        var completionSource = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        var queuedOperation = new QueuedWriteOperation(
            runAsync: async token =>
            {
                try
                {
                    var result = await operation(token).ConfigureAwait(false);
                    completionSource.TrySetResult(result);
                }
                catch (Exception ex)
                {
                    completionSource.TrySetException(ex);
                }
            },
            onCompletion: MarkWriteCompleted);

        try
        {
            await _writeQueue.Writer.WriteAsync(queuedOperation, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            completionSource.TrySetException(ex);
            MarkWriteCompleted();
        }

        return await completionSource.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessQueueAsync()
    {
        var reader = _writeQueue.Reader;
        var batch = new List<QueuedWriteOperation>(_options.WriteBatchSize);

        try
        {
            while (await reader.WaitToReadAsync(_queueCancellation.Token).ConfigureAwait(false))
            {
                if (!reader.TryRead(out var firstOperation))
                {
                    continue;
                }

                batch.Clear();
                batch.Add(firstOperation);

                while (batch.Count < _options.WriteBatchSize && reader.TryRead(out var operation))
                {
                    batch.Add(operation);
                }

                if (batch.Count < _options.WriteBatchSize && _options.BatchFlushIntervalMs > 0)
                {
                    await Task.Delay(_options.BatchFlushIntervalMs, _queueCancellation.Token).ConfigureAwait(false);
                    while (batch.Count < _options.WriteBatchSize && reader.TryRead(out var operation))
                    {
                        batch.Add(operation);
                    }
                }

                await Parallel.ForEachAsync(
                    batch,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = _options.MaxConcurrentWrites,
                        CancellationToken = _queueCancellation.Token
                    },
                    async (operation, token) =>
                    {
                        try
                        {
                            await operation.RunAsync(token).ConfigureAwait(false);
                        }
                        finally
                        {
                            operation.OnCompletion();
                        }
                    }).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static void ValidateOptions(AsyncProcessingOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.MaxConcurrentWrites <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.MaxConcurrentWrites), "MaxConcurrentWrites must be greater than zero.");
        }

        if (options.WriteBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.WriteBatchSize), "WriteBatchSize must be greater than zero.");
        }

        if (options.BatchFlushIntervalMs < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BatchFlushIntervalMs), "BatchFlushIntervalMs must be zero or greater.");
        }
    }

    private void MarkWriteCompleted()
    {
        if (Interlocked.Decrement(ref _pendingWrites) == 0)
        {
            lock (_flushLock)
            {
                _flushCompletionSource?.TrySetResult(true);
                _flushCompletionSource = null;
            }
        }
    }

    private void LogFireAndForgetFailure(Exception? exception, string operationName)
    {
        _logger.LogError(exception, "Fire-and-forget operation '{OperationName}' failed.", operationName);
    }

    public Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default) =>
        (_inner as IBackendMetadataProvider)?.GetBackendMetadataAsync(key, cancellationToken) ?? Task.FromResult<string?>(null);

    public void EvictFromCache(string key) => (_inner as ICacheEviction)?.EvictFromCache(key);

    private sealed class QueuedWriteOperation
    {
        public QueuedWriteOperation(Func<CancellationToken, Task> runAsync, Action onCompletion)
        {
            RunAsync = runAsync;
            OnCompletion = onCompletion;
        }

        public Func<CancellationToken, Task> RunAsync { get; }
        public Action OnCompletion { get; }
    }
}
