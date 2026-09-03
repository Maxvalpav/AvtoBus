using System.Linq.Expressions;
using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Hangfire;

/// <summary>
/// Фоновые задачи в стиле expression-API: `BackgroundJob.Enqueue(() => svc.Method(arg))` без классов сообщений.
/// Захватывает expression, сериализует вызов как сообщение `HangfireJobEnvelope`, шлет через IBus.
/// Продолжения: `ContinueWith`, батчи: `BatchJob.StartNew`.
/// </summary>
public static class BackgroundJob
{
    private static IBus? _bus;
    public static void Configure(IBus bus) => _bus = bus;

    public static string Enqueue(Expression<Action> methodCall)
        => EnqueueInternal(methodCall);

    public static string Enqueue<T>(Expression<Action<T>> methodCall) => EnqueueInternal(methodCall);
    public static string Enqueue<T>(Expression<Func<T, Task>> methodCall) => EnqueueInternal(methodCall);

    public static string ContinueWith(string parentId, Expression<Action> methodCall)
        => EnqueueInternal(methodCall, parentId);

    private static string EnqueueInternal(LambdaExpression expr, string? parentId = null)
    {
        if (_bus is null) throw new InvalidOperationException("BackgroundJob.Configure(IBus) не вызван. Вызови в Program.cs после AddAvtoBus.");
        var call = (MethodCallExpression)expr.Body;
        var job = new HangfireJobEnvelope
        {
            JobId = Guid.NewGuid().ToString("N"),
            ParentId = parentId,
            TypeName = call.Method.DeclaringType!.AssemblyQualifiedName!,
            MethodName = call.Method.Name,
            ArgsJson = System.Text.Json.JsonSerializer.Serialize(call.Arguments.Select(a => Expression.Lambda(a).Compile().DynamicInvoke())),
            EnqueuedAt = DateTimeOffset.UtcNow
        };
        var opts = new SendOptions();
        opts.WithHeader("avtobus.hangfire", "1");
        if (parentId is not null) opts.WithHeader("avtobus.hangfire.parent", parentId);
        // Раньше: fire-and-forget без observe — исключение сериализации/транспорта терялось,
        // а JobId возвращался до persist (потеря при crash). Теперь наблюдаем задачу:
        // в sync-фасаде исключение отправки не глотаем молча, а фиксируем в UnobservedGuard.
        _ = _bus.SendAsync(job, opts).AsTask().ContinueWith(
            t => System.Diagnostics.Trace.TraceWarning($"AvtoBus background send failed for job {job.JobId}: {t.Exception?.GetBaseException().Message}"),
            TaskContinuationOptions.OnlyOnFaulted);
        return job.JobId;
    }

    /// <summary>Async-версия: JobId возвращается только после persist в транспорт/outbox.</summary>
    public static async Task<string> EnqueueAsync(Expression<Action> methodCall, CancellationToken ct = default)
    {
        if (_bus is null) throw new InvalidOperationException("BackgroundJob.Configure(IBus) не вызван. Вызови в Program.cs после AddAvtoBus.");
        var call = (MethodCallExpression)methodCall.Body;
        var job = new HangfireJobEnvelope
        {
            JobId = Guid.NewGuid().ToString("N"),
            ParentId = null,
            TypeName = call.Method.DeclaringType!.AssemblyQualifiedName!,
            MethodName = call.Method.Name,
            ArgsJson = System.Text.Json.JsonSerializer.Serialize(call.Arguments.Select(a => Expression.Lambda(a).Compile().DynamicInvoke())),
            EnqueuedAt = DateTimeOffset.UtcNow
        };
        var opts = new SendOptions();
        opts.WithHeader("avtobus.hangfire", "1");
        await _bus.SendAsync(job, opts, ct).ConfigureAwait(false);
        return job.JobId;
    }
}

public sealed class HangfireJobEnvelope
{
    public required string JobId { get; init; }
    public string? ParentId { get; init; }
    public required string TypeName { get; init; }
    public required string MethodName { get; init; }
    public string? ArgsJson { get; init; }
    public DateTimeOffset EnqueuedAt { get; init; }
}

public sealed class BatchJob
{
    private readonly List<string> _ids = [];
    public BatchJob Enqueue(Expression<Action> call) { _ids.Add(BackgroundJob.Enqueue(call)); return this; }
    public IReadOnlyList<string> JobIds => _ids;
    public static BatchJob StartNew(Action<BatchJob> configure)
    {
        var b = new BatchJob();
        configure(b);
        return b;
    }
    public string AwaitBatch(string batchId) => BackgroundJob.Enqueue(() => Noop(batchId));
    private static void Noop(string _) { }
}

public static class HangfireExtensions
{
    public static BusConfigurator UseHangfireBridge(this BusConfigurator bus)
    {
        bus.Services.AddSingleton<HangfireJobHandler>();
        bus.AddConsumer<HangfireJobHandler>();
        return bus;
    }
}

public sealed class HangfireJobHandler : IConsumer<HangfireJobEnvelope>
{
    private readonly IServiceProvider _sp;
    public HangfireJobHandler(IServiceProvider sp) => _sp = sp;
    public async Task ConsumeAsync(ConsumeContext<HangfireJobEnvelope> ctx)
    {
        var env = ctx.Message;
        var type = Type.GetType(env.TypeName);
        if (type is null) throw new InvalidOperationException($"Type {env.TypeName} not found");
        var svc = _sp.GetRequiredService(type);
        var method = type.GetMethod(env.MethodName) ?? throw new InvalidOperationException($"Method {env.MethodName} not found");
        var args = env.ArgsJson is null ? [] : System.Text.Json.JsonSerializer.Deserialize<object[]>(env.ArgsJson) ?? [];
        var result = method.Invoke(svc, args);
        if (result is Task t) await t;
    }
}
