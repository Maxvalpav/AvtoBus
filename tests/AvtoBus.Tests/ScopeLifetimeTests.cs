using System.Collections.Concurrent;
using AvtoBus.Pipeline;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// DI-скоуп живёт одну попытку обработки (идея 203): весь пайплайн одной попытки
/// делит один инстанс scoped-сервиса, а следующая попытка получает новый скоуп.
/// </summary>
public class ScopeLifetimeTests
{
    [Fact]
    public async Task One_scope_shared_across_pipeline_for_single_attempt()
    {
        var sink = new ScopeProbeSink();

        await using var harness = await CreateHarnessAsync(sink);

        await harness.Bus.SendAsync(new ScopedCommand(Guid.NewGuid()));

        // Recorder срабатывает до нашего middleware, поэтому ждём записи всех шагов пайплайна.
        Assert.True(await harness.WaitUntilAsync(() => sink.Entries.Count == 2), "Сообщение не обработано");

        var entries = sink.Entries.ToArray();
        Assert.Equal(2, entries.Length);
        Assert.Contains(entries, entry => entry.Role == "middleware");
        Assert.Contains(entries, entry => entry.Role == "handler");
        Assert.Equal(entries[0].ScopeId, entries[1].ScopeId);
        Assert.All(entries, entry => Assert.Equal(1, entry.Attempt));
    }

    [Fact]
    public async Task Each_attempt_gets_a_fresh_scope()
    {
        var sink = new ScopeProbeSink();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddContract<ScopedCommand>();
                bus.AddConsumer<ScopedConsumer>();
                bus.Pipeline(pipeline =>
                {
                    pipeline.Use(new ScopeProbeMiddleware(sink));
                    pipeline.Use(new FailFirstAttemptMiddleware(1));
                });
            },
            services =>
            {
                services.AddSingleton(sink);
                services.AddScoped<ScopeProbe>();
            });

        await harness.Bus.SendAsync(new ScopedCommand(Guid.NewGuid()));

        // Первая попытка падает в нашем middleware (probe записывает её scope),
        // вторая проходит насквозь до хендлера.
        Assert.True(await harness.WaitUntilAsync(
            () => sink.Entries.Any(entry => entry.Role == "handler" && entry.Attempt == 2)),
            "Ретрай не выполнился");

        var entries = sink.Entries.ToArray();
        var firstMiddleware = entries.Single(entry => entry.Role == "middleware" && entry.Attempt == 1);
        var secondMiddleware = entries.Single(entry => entry.Role == "middleware" && entry.Attempt == 2);
        var secondHandler = entries.Single(entry => entry.Role == "handler" && entry.Attempt == 2);

        Assert.NotEqual(firstMiddleware.ScopeId, secondMiddleware.ScopeId);
        Assert.Equal(secondMiddleware.ScopeId, secondHandler.ScopeId);
    }

    private static async Task<AvtoBusTestHarness> CreateHarnessAsync(ScopeProbeSink sink)
    {
        var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddContract<ScopedCommand>();
                bus.AddConsumer<ScopedConsumer>();
                bus.Pipeline(pipeline => pipeline.Use(new ScopeProbeMiddleware(sink)));
            },
            services =>
            {
                services.AddSingleton(sink);
                services.AddScoped<ScopeProbe>();
            });

        return harness;
    }
}

/// <summary>Общая запись: какой scope увидел какой шаг пайплайна на какой попытке.</summary>
public sealed class ScopeProbeSink
{
    public ConcurrentQueue<(int ScopeId, int Attempt, string Role)> Entries { get; } = new();
}

/// <summary>Scoped-сервис: уникальный идентификатор на каждый созданный скоуп.</summary>
public sealed class ScopeProbe
{
    private static int _nextId;

    public int InstanceId { get; } = Interlocked.Increment(ref _nextId);
}

public sealed class ScopeProbeMiddleware(ScopeProbeSink sink) : IBusMiddleware
{
    public ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var probe = context.Services.GetRequiredService<ScopeProbe>();
        sink.Entries.Enqueue((probe.InstanceId, context.Envelope.DeliveryAttempt, "middleware"));
        return next(context);
    }
}

/// <summary>Падает на первой попытке доставки, чтобы проверить создание нового скоупа при ретрае.</summary>
public sealed class FailFirstAttemptMiddleware(int fails) : IBusMiddleware
{
    private int _remaining = fails;

    public ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        if (context.Envelope.DeliveryAttempt == 1 && Interlocked.Decrement(ref _remaining) >= 0)
            throw new InvalidOperationException("Намеренный сбой первой попытки");

        return next(context);
    }
}

public sealed class ScopedConsumer(ScopeProbe probe, ScopeProbeSink sink) : IConsumer<ScopedCommand>
{
    public Task ConsumeAsync(ConsumeContext<ScopedCommand> context)
    {
        sink.Entries.Enqueue((probe.InstanceId, context.Envelope.DeliveryAttempt, "handler"));
        return Task.CompletedTask;
    }
}
