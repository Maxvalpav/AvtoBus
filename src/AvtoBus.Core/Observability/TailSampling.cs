using System.Diagnostics;
using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Observability;

/// <summary>
/// Tail Sampling (Tempo/Jaeger): сохраняет 100% ошибок/late/D LQ, семплит 1% успехов.
/// Вместо head-sampling (теряет ошибки) — решение после обработки. DLQ хранит traceId для клика в Grafana.
/// Аналог: Grafana Tempo tail sampling, Jaeger adaptive sampling, OTEL `TraceIdRatioBased` + `ParentBased`.
/// </summary>
public sealed class TailSamplingOptions
{
    public double SuccessRatio { get; set; } = 0.01; // 1% успехов
    public bool AlwaysSampleErrors { get; set; } = true;
    public bool AlwaysSampleDlq { get; set; } = true;
}

public sealed class TailSampler(TailSamplingOptions opts)
{
    public bool ShouldSample(ConsumeContext ctx)
    {
        if (ctx.Outcome is ConsumeOutcome.DeadLettered or ConsumeOutcome.Failed && opts.AlwaysSampleDlq) return true;
        // Проверка ошибки по activity статусу
        var act = Activity.Current;
        if (act is not null && act.Status == ActivityStatusCode.Error && opts.AlwaysSampleErrors) return true;
        return Random.Shared.NextDouble() < opts.SuccessRatio;
    }
    public void Apply(ConsumeContext ctx)
    {
        var should = ShouldSample(ctx);
        Activity.Current?.SetTag("avtobus.sampled", should ? "true" : "false");
        // DLQ обогащаем traceId для перехода в Tempo
        if (ctx.Outcome is ConsumeOutcome.DeadLettered && Activity.Current?.TraceId.ToString() is { } traceId)
        {
            ctx.Envelope.Headers.TryGetValue("avtobus.traceId", out _); // ensure mutable via Items
            ctx.Items["avtobus.dlq.traceId"] = traceId;
        }
    }
}

public static class TailSamplingExtensions
{
    public static BusConfigurator UseTailSampling(this BusConfigurator bus, Action<TailSamplingOptions>? configure = null)
    {
        var opts = new TailSamplingOptions();
        configure?.Invoke(opts);
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton<TailSampler>();
        bus.Pipeline(b => b.Use<TailSamplingMiddleware>());
        return bus;
    }
}

public sealed class TailSamplingMiddleware(TailSampler sampler) : AvtoBus.Pipeline.IBusMiddleware
{
    public async ValueTask InvokeAsync(ConsumeContext ctx, AvtoBus.Pipeline.BusDelegate next)
    {
        await next(ctx).ConfigureAwait(false);
        sampler.Apply(ctx);
        // OTel: если не sampled — активность можно дропнуть, но в SDK это делает Sampler. Здесь только тег.
        if (!sampler.ShouldSample(ctx))
            Activity.Current?.ActivityTraceFlags &= ~ActivityTraceFlags.Recorded;
    }
}

/// <summary>DLQ обогащение traceId для прямой ссылки Tempo/Grafana.</summary>
public static class DlqTraceEnricher
{
    public static IReadOnlyDictionary<string, string> Enrich(Envelope envelope)
    {
        var traceId = Activity.Current?.TraceId.ToString();
        if (traceId is null) return envelope.Headers;
        var d = new Dictionary<string, string>(envelope.Headers) { ["avtobus.traceId"] = traceId, ["avtobus.traceParent"] = envelope.TraceParent ?? "" };
        return d;
    }
}
