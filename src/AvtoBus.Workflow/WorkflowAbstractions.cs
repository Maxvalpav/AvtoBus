using AvtoBus.Abstractions;

namespace AvtoBus.Workflow;

public sealed class WorkflowOptions
{
    public TimeSpan SnapshotInterval { get; set; } = TimeSpan.FromMinutes(5);
    public int SnapshotEventThreshold { get; set; } = 100;
}

public interface IWorkflowContext
{
    DateTimeOffset Now { get; }
    Guid NewGuid();
    Task CreateTimer(TimeSpan delay, CancellationToken ct = default);
    Task<T> ExecuteActivityAsync<T>(Func<Task<T>> activity, ActivityOptions? options = null);
    /// <summary>
    /// Синхронный фасад прошлого: блокирует поток. Используйте <see cref="ContinueAsNewAsync"/>.
    /// </summary>
    [Obsolete("Используйте ContinueAsNewAsync — sync-версия блокирует поток.")]
    void ContinueAsNew(object input);
    /// <summary>Перезапуск workflow с новым входом (async-версия, не блокирует).</summary>
    Task ContinueAsNewAsync(object input, CancellationToken ct = default);

    // Шаг сна до даты + ожидание внешнего события с таймаутом
    Task SleepUntil(DateTimeOffset at, CancellationToken ct = default);
    Task<T> WaitForEventAsync<T>(string eventName, TimeSpan? timeout = null, CancellationToken ct = default);
    Task SendSignalAsync(string workflowId, string signalName, object payload, CancellationToken ct = default);
}

public sealed class ActivityOptions
{
    public TimeSpan StartToCloseTimeout { get; init; } = TimeSpan.FromMinutes(2);
    public TimeSpan ScheduleToCloseTimeout { get; init; } = TimeSpan.FromMinutes(5);
    public int MaxAttempts { get; init; } = 3;
}

public abstract class AvtoWorkflow<TInput, TOutput>
{
    public abstract Task<TOutput> RunAsync(TInput input, IWorkflowContext context);
}

public sealed class WorkflowInstanceRunner
{
    private readonly IWorkflowStore _store;
    private readonly TimeProvider _clock;
    private readonly IScheduledStore? _scheduled;
    private readonly IAvtoUnitOfWork? _uow;

    public WorkflowInstanceRunner(IWorkflowStore store, TimeProvider clock, IScheduledStore? scheduled = null, IAvtoUnitOfWork? uow = null)
    {
        _store = store;
        _clock = clock;
        _scheduled = scheduled;
        _uow = uow;
    }

    public async Task<string> StartAsync<TInput>(string workflowType, TInput input, CancellationToken ct)
    {
        var id = $"{workflowType}:{Guid.NewGuid():N}";
        var instance = new WorkflowInstance
        {
            Id = id,
            WorkflowType = workflowType,
            Status = "Running",
            Version = 0,
        };
        await _store.SaveAsync(instance, ct);
        await _store.AppendHistoryAsync([new WorkflowHistoryEvent
        {
            WorkflowId = id,
            Sequence = 0,
            EventType = "WorkflowStarted",
            Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(input),
            CreatedAt = _clock.GetUtcNow(),
        }], ct);
        if (_uow is not null) await _uow.CommitAsync(ct);
        return id;
    }

    public async Task SignalAsync(string workflowId, string signalName, object payload, CancellationToken ct)
        => await SendSignalStatic(_store, workflowId, signalName, payload, ct, _clock, _uow);

    internal static async Task SendSignalStatic(IWorkflowStore store, string workflowId, string signalName, object payload, CancellationToken ct, TimeProvider clock, IAvtoUnitOfWork? uow = null)
    {
        var history = await store.ReadHistoryAsync(workflowId, 0, ct);
        var seq = history.Count;
        await store.AppendHistoryAsync([new WorkflowHistoryEvent
        {
            WorkflowId = workflowId,
            Sequence = seq,
            EventType = $"Signal:{signalName}",
            Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(payload),
            CreatedAt = clock.GetUtcNow(),
        }], ct);
        if (uow is not null) await uow.CommitAsync(ct);
    }

    public IWorkflowContext CreateContext(string workflowId)
        // Синхронный фасад для DI/конструкторов: уходим на пул, чтобы не дедлочить
        // чужой SynchronizationContext. Из async-кода используй CreateContextAsync.
        => Task.Run(() => CreateContextAsync(workflowId)).GetAwaiter().GetResult();

    /// <summary>Async-версия <see cref="CreateContext"/> — используй её из async-кода, чтобы не было sync-over-async.</summary>
    public async Task<IWorkflowContext> CreateContextAsync(string workflowId, CancellationToken ct = default)
    {
        var hist = await _store.ReadHistoryAsync(workflowId, 0, ct).ConfigureAwait(false);
        var max = hist.Count > 0 ? hist.Max(h => h.Sequence) : -1;
        return new DefaultWorkflowContext(workflowId, _store, _scheduled, _clock, max);
    }

    private sealed class DefaultWorkflowContext(string workflowId, IWorkflowStore store, IScheduledStore? scheduled, TimeProvider clock, long initialSeq) : IWorkflowContext
    {
        private long _seq = initialSeq;
        public DateTimeOffset Now => clock.GetUtcNow();
        public Guid NewGuid() => Guid.NewGuid();
        public async Task ContinueAsNewAsync(object input, CancellationToken ct = default)
        {
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = "ContinueAsNew", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(input), CreatedAt = clock.GetUtcNow() }], ct).ConfigureAwait(false);
        }

        [Obsolete("Используйте ContinueAsNewAsync — sync-версия блокирует поток.")]
        public void ContinueAsNew(object input)
        {
            // Legacy sync-фасад: оставлен для совместимости, новым кодом не пользоваться.
            Task.Run(() => ContinueAsNewAsync(input)).GetAwaiter().GetResult();
        }

        public Task SleepUntil(DateTimeOffset at, CancellationToken ct = default)
        {
            var delay = at - clock.GetUtcNow();
            if (delay <= TimeSpan.Zero) return Task.CompletedTask;
            return CreateTimer(delay, ct);
        }

        public async Task<T> WaitForEventAsync<T>(string eventName, TimeSpan? timeout = null, CancellationToken ct = default)
        {
            var seq = Interlocked.Increment(ref _seq);
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = seq, EventType = $"WaitForEvent:{eventName}", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(eventName), CreatedAt = clock.GetUtcNow() }], ct).ConfigureAwait(false);
            var deadline = timeout.HasValue ? clock.GetUtcNow() + timeout.Value : DateTimeOffset.MaxValue;
            var pollDelay = TimeSpan.FromMilliseconds(50);
            while (clock.GetUtcNow() < deadline)
            {
                ct.ThrowIfCancellationRequested();
                var history = await store.ReadHistoryAsync(workflowId, seq, ct).ConfigureAwait(false);
                var sig = history.FirstOrDefault(h => h.EventType == $"Signal:{eventName}");
                if (sig is not null)
                {
                    await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = $"EventReceived:{eventName}", Payload = sig.Payload, CreatedAt = clock.GetUtcNow() }], ct).ConfigureAwait(false);
                    return System.Text.Json.JsonSerializer.Deserialize<T>(sig.Payload)!;
                }
                if (seq == 0)
                {
                    var full = await store.ReadHistoryAsync(workflowId, 0, ct).ConfigureAwait(false);
                    sig = full.FirstOrDefault(h => h.EventType == $"Signal:{eventName}");
                    if (sig is not null)
                    {
                        await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = $"EventReceived:{eventName}", Payload = sig.Payload, CreatedAt = clock.GetUtcNow() }], ct).ConfigureAwait(false);
                        return System.Text.Json.JsonSerializer.Deserialize<T>(sig.Payload)!;
                    }
                }
                await Task.Delay(pollDelay, clock, ct).ConfigureAwait(false);
                pollDelay = TimeSpan.FromMilliseconds(Math.Min(pollDelay.TotalMilliseconds * 1.5, 500));
                if (scheduled is null && timeout is null) break;
            }
            throw new TimeoutException($"WaitForEvent '{eventName}' timed out after {timeout}");
        }

        public Task SendSignalAsync(string targetWorkflowId, string signalName, object payload, CancellationToken ct = default)
            => WorkflowInstanceRunner.SendSignalStatic(store, targetWorkflowId, signalName, payload, ct, clock);

        public async Task CreateTimer(TimeSpan delay, CancellationToken ct = default)
        {
            var seq = Interlocked.Increment(ref _seq);
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = seq, EventType = "TimerCreated", Payload = BitConverter.GetBytes(delay.Ticks), CreatedAt = clock.GetUtcNow() }], ct).ConfigureAwait(false);
            if (scheduled is not null)
            {
                var env = new AvtoEnvelope { MessageId = Guid.NewGuid(), MessageType = "WorkflowTimerFired", SchemaName = "workflow.timer-fired", SchemaVersion = 1, CreatedAt = clock.GetUtcNow(), Body = BitConverter.GetBytes(delay.Ticks), Headers = new() };
                await scheduled.AddAsync([new ScheduledRecord { Id = Guid.NewGuid(), Envelope = env, Destination = $"workflow:{workflowId}", Transport = TransportNames.InMemory, ScheduledAt = clock.GetUtcNow() + delay }], ct).ConfigureAwait(false);
            }
            else
            {
                if (delay > TimeSpan.FromSeconds(2))
                {
                    await Task.Yield();
                }
                else
                {
                    await Task.Delay(delay, clock, ct).ConfigureAwait(false);
                }
            }
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = "TimerFired", Payload = BitConverter.GetBytes(delay.Ticks), CreatedAt = clock.GetUtcNow() }], ct).ConfigureAwait(false);
        }
        public async Task<T> ExecuteActivityAsync<T>(Func<Task<T>> activity, ActivityOptions? options = null)
        {
            options ??= new ActivityOptions();
            var seq = Interlocked.Increment(ref _seq);
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = seq, EventType = "ActivityScheduled", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(typeof(T).Name), CreatedAt = clock.GetUtcNow() }], CancellationToken.None);
            Exception? last = null;
            for (int attempt = 1; attempt <= options.MaxAttempts; attempt++)
            {
                try
                {
                    using var cts = new CancellationTokenSource(options.StartToCloseTimeout);
                    var result = await activity().WaitAsync(cts.Token);
                    await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = "ActivityCompleted", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(result), CreatedAt = clock.GetUtcNow() }], CancellationToken.None);
                    return result;
                }
                catch (Exception ex)
                {
                    last = ex;
                    if (attempt < options.MaxAttempts)
                    {
                        // Backoff с cap и jitter вместо фиксированных 100*attempt без токена.
                        var delayMs = Math.Min(100 * attempt, 5_000) * (0.8 + Random.Shared.NextDouble() * 0.4);
                        try { await Task.Delay(TimeSpan.FromMilliseconds(delayMs), clock); }
                        catch (OperationCanceledException) { break; }
                    }
                }
            }
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = "ActivityFailed", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(last?.Message ?? "unknown"), CreatedAt = clock.GetUtcNow() }], CancellationToken.None);
            throw last ?? new InvalidOperationException("Activity failed");
        }
    }
}
