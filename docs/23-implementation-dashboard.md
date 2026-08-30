# 🖥 Реализация: AvtoBus Dashboard (Blazor)

> **Design draft.** UI и management API не подключены к работающему backend и не проверены в production-среде.

Пакет `AvtoBus.Dashboard` — встраиваемый Blazor UI без отдельного разворачивания.
Подключается как `app.MapAvtoBusDashboard("/bus");`

## 1. Что отображается

- **Overview**: throughput, p95/p99 critical time, DLQ count, саги в процессе
- **Queues**: глубина, консьюмеры, lag, throttle
- **Messages**: live-tail + поиск по MessageId/CorrelationId
- **Dead-letter queue**: просмотр, редактирование, replay (rate-limited)
- **Sagas**: список инстансов, таймлайны, state, приостановка
- **Outbox**: pending/claimed/sent, чистка
- **Topology**: интерактивный Mermaid-граф сервисов и очередей
- **DLQ replay с фильтрами**

## 2. Минимальный сервис дашборда

```csharp
// AvtoBus.Dashboard/DashboardService.cs
public sealed class DashboardService
{
    private readonly ITransportAdmin _admin;       // получает метрики от брокера
    private readonly IOutboxAdmin _outbox;
    private readonly ISagaAdmin _saga;
    private readonly IMessageStore _audit;
    private readonly TimeProvider _clock;

    public DashboardService(ITransportAdmin admin, IOutboxAdmin outbox,
        ISagaAdmin saga, IMessageStore audit, TimeProvider clock)
        => (_admin, _outbox, _saga, _audit, _clock) = (admin, outbox, saga, audit, clock);

    public async Task<OverviewVm> GetOverview(CancellationToken ct)
    {
        var queues = await _admin.ListQueuesAsync(ct);
        return new OverviewVm(
            TotalPending: queues.Sum(q => q.Messages),
            TotalConsumers: queues.Sum(q => q.ConsumerCount),
            DlqCount: queues.Where(q => q.IsDlq).Sum(q => q.Messages),
            ActiveSagas: await _saga.CountActiveAsync(ct),
            OutboxPending: await _outbox.CountPendingAsync(ct),
            CriticalTimeP95: TimeSpan.FromMilliseconds(queues.Average(q => q.ConsumeLatencyP95Ms)));
    }

    public async Task<IReadOnlyList<QueueVm>> ListQueues(CancellationToken ct)
    {
        var qs = await _admin.ListQueuesAsync(ct);
        return qs.Select(q => new QueueVm(
            Name: q.Name,
            Messages: q.Messages,
            Consumers: q.ConsumerCount,
            RatePerSecond: q.IngressPerSec,
            ConsumeLatencyP95Ms: q.ConsumeLatencyP95Ms,
            DeadLetterCount: q.DeadLetterCount,
            IsDlq: q.IsDlq
        )).ToImmutableArray();
    }

    public async Task<IReadOnlyList<DeadLetterVm>> ListDeadLetters(string queue, int skip, int take, CancellationToken ct)
    {
        var msgs = await _admin.ListDeadLettersAsync(queue, skip, take, ct);
        return msgs.Select(m => new DeadLetterVm(m.Id, m.MessageType, m.Envelope.MessageId,
            m.Error.Message, m.Error.Stacktrace, m.OccurredAt)).ToImmutableArray();
    }

    public async Task<int> ReplayDeadLetters(ReplayRequest req, CancellationToken ct)
    {
        var msgs = await _admin.QueryDeadLettersAsync(req.Queue, req.Filter, ct);
        var throttler = new SemaphoreSlim(req.MaxParallelism);
        var replayed = 0;
        await Parallel.ForEachAsync(msgs, ct, async (m, t) =>
        {
            await throttler.WaitAsync(t);
            try
            {
                if (req.Patch is not null)
                    m = await req.Patch.Apply(m);
                await _admin.RepublishAsync(m, t);
                await _admin.DeleteDeadLetterAsync(m.Id, t);
                Interlocked.Increment(ref replayed);
            }
            finally { throttler.Release(); }
        });
        return replayed;
    }
}
```

## 3. ITransportAdmin для RabbitMQ

```csharp
internal sealed class RabbitMqAdmin : ITransportAdmin
{
    private readonly ManagementApiClient _mgmt;   // HTTP API RabbitMQ
    private readonly IConnectionFactory _factory;
    private readonly RabbitMqTopology _topology;

    public async Task<IReadOnlyList<QueueStat>> ListQueuesAsync(CancellationToken ct)
    {
        var queues = await _mgmt.GetQueuesAsync(vhost: "/", ct);
        return queues
            .Where(q => !q.Name.StartsWith("amq."))
            .Select(q => new QueueStat(
                Name: q.Name,
                Messages: q.Messages,
                ConsumerCount: q.Consumers,
                IngressPerSec: q.MessageStats?.PublishDetails?.Rate ?? 0,
                ConsumeLatencyP95Ms: q.ConsumerStats?.AckDetails.GetPercentile(0.95) ?? 0,
                DeadLetterCount: 0,
                IsDlq: q.Name.EndsWith(".error") || q.Name.EndsWith(".poison")))
            .ToImmutableArray();
    }

    public async Task<IReadOnlyList<DeadLetter>> ListDeadLettersAsync(string queue, int skip, int take, CancellationToken ct)
    {
        // Получаем до take сообщений из DLQ через basic-get и возвращаем
        await using var conn = await _factory.CreateConnectionAsync("avtobus-dashboard");
        await using var ch = await conn.CreateChannelAsync();
        var result = new List<DeadLetter>();
        for (int i = 0; i < take; i++)
        {
            var get = await ch.BasicGetAsync(queue, autoAck: false, ct);
            if (get is null) break;
            var dlq = ParseDeadLetter(get);
            result.Add(dlq);
            // не ack'аем, только при replay
            await ch.BasicNackAsync(get.DeliveryTag, multiple: false, requeue: true, ct);
        }
        return result;
    }
}
```

## 4. Маршрут для minimal API

```csharp
public static class DashboardEndpoint
{
    public static IEndpointRouteBuilder MapAvtoBusDashboard(this IEndpointRouteBuilder app, string pattern = "/bus")
    {
        var gr = app.MapGroup(pattern).RequireAuthorization(policy: "AvtoBusDashboard");

        gr.MapGet("api/overview", (DashboardService svc, CancellationToken ct) => svc.GetOverview(ct));
        gr.MapGet("api/queues", (DashboardService svc, CancellationToken ct) => svc.ListQueues(ct));
        gr.MapGet("api/dlq/{queue}", (string queue, [AsParameters] Paging p, DashboardService svc, CancellationToken ct)
            => svc.ListDeadLetters(queue, p.Skip, p.Take, ct));
        gr.MapPost("api/dlq/{queue}/replay", (string queue, ReplayRequest req, DashboardService svc, CancellationToken ct)
            => svc.ReplayDeadLetters(req with { Queue = queue }, ct));

        gr.MapGet("api/sagas", (DashboardService svc, [AsParameters] SagaFilter f, CancellationToken ct)
            => svc.ListSagas(f, ct));
        gr.MapGet("api/sagas/{id}", (Guid id, DashboardService svc, CancellationToken ct)
            => svc.GetSaga(id, ct));
        gr.MapPost("api/sagas/{id}/pause", (Guid id, DashboardService svc, CancellationToken ct)
            => svc.PauseSaga(id, ct));
        gr.MapPost("api/sagas/{id}/resume", (Guid id, DashboardService svc, CancellationToken ct)
            => svc.ResumeSaga(id, ct));

        gr.MapGet("api/topology", (DashboardService svc, CancellationToken ct) => svc.GetTopologyGraph(ct));

        // Blazor / SPA fallback
        app.MapFallbackToFile("{**path:nonfile}", "dashboard/index.html",
            o => o.AllowAnonymous().Add(endpoint => endpoint.Metadata.Add(new DashboardAssetMetadata())));

        return app;
    }
}
```

## 5. Live-tail через Server-Sent Events

```csharp
gr.MapGet("api/tail/{queue}", async (
    string queue,
    ITransportAdmin admin,
    CancellationToken ct,
    HttpContext ctx) =>
{
    ctx.Response.ContentType = "text/event-stream";
    ctx.Response.Headers.CacheControl = "no-cache";

    await using var sub = await admin.SubscribeAsync(queue, ct);
    await foreach (var ev in sub.ReadAllAsync(ct))
    {
        var json = JsonSerializer.Serialize(new TailEvent(ev.MessageType, ev.MessageId, ev.SentAt));
        await ctx.Response.WriteAsync($"data: {json}\n\n", ct);
        await ctx.Response.Body.FlushAsync(ct);
    }
});
```

## 6. Razor/Blazor компонент очереди (упрощённый)

```razor
<!-- AvtoBus.Dashboard/Components/Queues.razor -->
@page "/queues"
@inject DashboardService Bus

<h3 class="text-lg font-semibold">Queues</h3>

@if (_queues is null) { <p>Loading…</p> }
else
{
    <table class="min-w-full text-sm">
        <thead>
            <tr class="text-slate-400">
                <th class="text-left">Queue</th><th>Pending</th><th>Consumers</th>
                <th>Rate/s</th><th>p95 ms</th><th>DLQ</th>
            </tr>
        </thead>
        <tbody>
        @foreach (var q in _queues)
        {
            <tr class="border-t border-slate-800">
                <td class="font-mono">@q.Name</td>
                <td>@q.Messages.N0</td>
                <td>@q.Consumers</td>
                <td>@q.RatePerSecond.ToString("0.0")</td>
                <td>@q.ConsumeLatencyP95Ms.ToString("0")</td>
                <td>
                    @if (q.DeadLetterCount > 0)
                    {
                        <a href="/dlq/@q.Name" class="text-rose-400">@q.DeadLetterCount</a>
                    }
                </td>
            </tr>
        }
        </tbody>
    </table>
}

@code {
    private IReadOnlyList<QueueVm>? _queues;
    protected override async Task OnInitializedAsync()
        => _queues = await Bus.ListQueues(default);
}
```

## 7. Редактирование payload перед replay (Patch UI)

```razor
@page "/dlq/{queue}/{id}"
@inject DashboardService Bus

<div class="space-y-3">
  <h3>Dead letter @Id in @Queue</h3>
  <pre class="rounded bg-slate-900 p-3 text-xs">@_dlq?.Error</pre>
  <label class="block text-sm">Body (edit then replay)</label>
  <textarea rows="18" class="w-full bg-slate-900 text-slate-200 p-2 font-mono text-xs" @bind="_body" />
  <button class="bg-emerald-600 px-4 py-2 rounded text-white" @onclick="Replay">Replay</button>
</div>

@code {
    [Parameter] public string Queue { get; set; } = "";
    [Parameter] public string Id { get; set; } = "";
    private DeadLetterVm? _dlq;
    private string _body = "";

    protected override async Task OnInitializedAsync()
    {
        _dlq = await Bus.GetDeadLetter(Queue, Id);
        _body = _dlq.PrettyBody;
    }

    private async Task Replay()
    {
        var patch = new JsonPatchBody(_body);
        await Bus.ReplayDeadLetters(new ReplayRequest(Queue, new AllFilter(), MaxParallelism: 1, Patch: patch));
        Navigation.NavigateTo($"/dlq/{Queue}");
    }
}
```

## 8. Интерактивный граф топологии (Mermaid)

```csharp
public async Task<string> GetTopologyGraph(CancellationToken ct)
{
    var nodes = new List<string>();
    var edges = new List<string>();
    var routes = await _admin.ListRoutesAsync(ct);

    foreach (var s in routes.Services)
        nodes.Add($"    {Mangle(s)}[\"🟢 {s}\"]");
    foreach (var q in routes.Queues)
        nodes.Add($"    {Mangle(q)}[\"📥 {q}\"]");

    foreach (var (service, queue) in routes.Subscriptions)
        edges.Add($"    {Mangle(queue)} --> {Mangle(service)}");
    foreach (var (service, queue) in routes.Publishes)
        edges.Add($"    {Mangle(service)} --> {Mangle(queue)}");

    var sb = new StringBuilder();
    sb.AppendLine("flowchart LR");
    foreach (var n in nodes) sb.AppendLine(n);
    foreach (var e in edges) sb.AppendLine(e);
    return sb.ToString();
}
```

В UI компонент просто:

```razor
<div class="mermaid">@_mermaid</div>
```

c подключённым `mermaid.min.js`.

## 9. Защита и аудит

- Доступ по ASP.NET policy `AvtoBusDashboard`
- PII-поля маскируются согласно `[PersonalData]`-атрибутам (идея 124)
- Все действия (replay, pause, edit) пишутся в `avtobus_dashboard_audit` (user, action, target, body diff)
- В проде по умолчанию отключены редактирование, publish и live-tail с телами. Включение требует явного `options.AllowDangerousOperationsInProduction = true` и отдельной authorization policy (идея 482).

## 10. Подключение

```csharp
// Program.cs
builder.Services.AddAvtoBusDashboard(o =>
{
    o.RoutePrefix = "/bus";
    o.AllowDangerousOperationsInProduction = false;
    o.PolicyName = "RequireAdminRole";
    o.MaxLiveTailBytesPerSecond = 2 * 1024 * 1024; // 2 MB/s, не убьём брокер
});
```

На любом развороте: добавил NuGet + `MapAvtoBusDashboard` — полноценный UI мониторинга готов, без отдельного деплоя (как Swagger).
