using AvtoBus.Abstractions;

namespace AvtoBus.Workflow;

/// <summary>
/// Temporal Queries/Updates + Schedules порт (Go/Java): синхронный запрос состояния workflow + durable cron.
/// Query — read-only `workflow.Query("balance")` без изменения истории. Update — валидируемый сигнал с ответом.
/// Schedule — cron с backfill и jitter как у Temporal `ScheduleSpec`, в отличие от `CronRegistry` без backfill.
/// </summary>
public interface IWorkflowQueryable
{
    Task<T> QueryAsync<T>(string workflowId, string queryName, CancellationToken ct = default);
    Task<T> UpdateAsync<T>(string workflowId, string updateName, object input, CancellationToken ct = default);
}

public sealed class WorkflowScheduleSpec
{
    public string Cron { get; init; } = "* * * * *";
    public TimeSpan Jitter { get; init; } = TimeSpan.Zero;
    public bool Backfill { get; init; } = false;
    public DateTimeOffset? StartAt { get; init; }
    public DateTimeOffset? EndAt { get; init; }
    public string WorkflowType { get; init; } = "";
}

public sealed class WorkflowScheduleHandle(string id, WorkflowScheduleSpec spec, IWorkflowStore store, TimeProvider clock)
{
    public string Id => id;
    public WorkflowScheduleSpec Spec => spec;
    public async Task TriggerManuallyAsync(object input, CancellationToken ct)
    {
        await store.AppendHistoryAsync([new WorkflowHistoryEvent
        {
            WorkflowId = id, Sequence = 0, EventType = "ScheduleManualTrigger",
            Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(input), CreatedAt = clock.GetUtcNow()
        }], ct);
    }
}

public sealed class WorkflowQueryRegistry
{
    private readonly Dictionary<(string workflowId, string query), Func<object?, object?>> _queries = new();
    public void Register<TState, TRes>(string queryName, Func<TState, TRes> handler) where TState : class
        => _queries[("*", queryName)] = state => state is TState s ? handler(s) : default;
    public bool TryGet(string workflowId, string query, out Func<object?, object?>? h) => _queries.TryGetValue((workflowId, query), out h) || _queries.TryGetValue(("*", query), out h);
}
