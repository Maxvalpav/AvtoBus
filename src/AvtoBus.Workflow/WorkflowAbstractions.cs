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
    void ContinueAsNew(object input);

    // Inngest (JS) `step.sleepUntil(date)` + Temporal (Go) `workflow.SleepUntil` + `AwaitWithTimeout`
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

    public IWorkflowContext CreateContext(string workflowId) => new DefaultWorkflowContext(workflowId, _store, _scheduled, _clock);

    private sealed class DefaultWorkflowContext(string workflowId, IWorkflowStore store, IScheduledStore? scheduled, TimeProvider clock) : IWorkflowContext
    {
        private long _seq;
        public DateTimeOffset Now => clock.GetUtcNow();
        public Guid NewGuid() => Guid.NewGuid();
        public void ContinueAsNew(object input)
        {
            // Синхронный фасад: реальный движок делает ресет исполнения; Wait() — дедлок под SynchronizationContext.
            // Используем GetAwaiter().GetResult() с ConfigureAwait(false) вне контекста.
            store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = "ContinueAsNew", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(input), CreatedAt = clock.GetUtcNow() }], CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
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
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = seq, EventType = $"WaitForEvent:{eventName}", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(eventName), CreatedAt = clock.GetUtcNow() }], ct);
            // Poll history for signal — Inngest `step.waitForEvent` + Temporal `workflow.GetSignalChannel`
            var deadline = timeout.HasValue ? clock.GetUtcNow() + timeout.Value : DateTimeOffset.MaxValue;
            while (clock.GetUtcNow() < deadline)
            {
                ct.ThrowIfCancellationRequested();
                var history = await store.ReadHistoryAsync(workflowId, 0, ct);
                var sig = history.FirstOrDefault(h => h.EventType == $"Signal:{eventName}");
                if (sig is not null)
                {
                    await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = $"EventReceived:{eventName}", Payload = sig.Payload, CreatedAt = clock.GetUtcNow() }], ct);
                    return System.Text.Json.JsonSerializer.Deserialize<T>(sig.Payload)!;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
                if (scheduled is null && timeout is null) break; // без durable store — не ждём вечно в тесте
            }
            throw new TimeoutException($"WaitForEvent '{eventName}' timed out after {timeout}");
        }

        public Task SendSignalAsync(string targetWorkflowId, string signalName, object payload, CancellationToken ct = default)
            => WorkflowInstanceRunner.SendSignalStatic(store, targetWorkflowId, signalName, payload, ct, clock);

        public async Task CreateTimer(TimeSpan delay, CancellationToken ct = default)
        {
            var seq = Interlocked.Increment(ref _seq);
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = seq, EventType = "TimerCreated", Payload = BitConverter.GetBytes(delay.Ticks), CreatedAt = clock.GetUtcNow() }], ct);
            if (scheduled is not null)
            {
                // Durable timer via scheduled store;SchedulerService will fire it as message
                var env = new AvtoEnvelope { MessageId = Guid.NewGuid(), MessageType = "WorkflowTimerFired", SchemaName = "workflow.timer-fired", SchemaVersion = 1, CreatedAt = clock.GetUtcNow(), Body = BitConverter.GetBytes(delay.Ticks), Headers = new() };
                await scheduled.AddAsync([new ScheduledRecord { Id = Guid.NewGuid(), Envelope = env, Destination = $"workflow:{workflowId}", Transport = "inmemory", ScheduledAt = clock.GetUtcNow() + delay }], ct);
            }
            else
            {
                await Task.Delay(delay, ct);
            }
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = "TimerFired", Payload = BitConverter.GetBytes(delay.Ticks), CreatedAt = clock.GetUtcNow() }], ct);
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
                catch (Exception ex) { last = ex; if (attempt < options.MaxAttempts) await Task.Delay(TimeSpan.FromMilliseconds(100 * attempt)); }
            }
            await store.AppendHistoryAsync([new WorkflowHistoryEvent { WorkflowId = workflowId, Sequence = Interlocked.Increment(ref _seq), EventType = "ActivityFailed", Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(last?.Message ?? "unknown"), CreatedAt = clock.GetUtcNow() }], CancellationToken.None);
            throw last ?? new InvalidOperationException("Activity failed");
        }
    }
}
