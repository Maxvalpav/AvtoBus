using System.Diagnostics;
using AvtoBus.Observability;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Pipeline;

/// <summary>
/// Собирает цепочку middleware в единый делегат. Композиция происходит один раз при старте,
/// на горячем пути остаётся только вызов готовой цепочки. Каждый шаг замеряется в
/// <c>avtobus.pipeline.step.duration</c> — «водопад» обработки (идея 334).
/// </summary>
public sealed class PipelineBuilder
{
    private readonly List<Func<BusDelegate, BusDelegate>> _components = [];

    /// <summary>Добавляет уже созданный экземпляр middleware (синглтон на всё приложение).</summary>
    public PipelineBuilder Use(IBusMiddleware middleware)
    {
        var name = middleware.GetType().Name;
        _components.Add(next => context => Timed(next, context, name, middleware));
        return this;
    }

    public PipelineBuilder Use(Func<ConsumeContext, BusDelegate, ValueTask> middleware)
        => Use(new DelegateMiddleware(middleware));

    /// <summary>
    /// Добавляет middleware, резолвимый из scoped-контейнера сообщения.
    /// Нужен, когда шагу требуются scoped-зависимости (DbContext, репозитории).
    /// </summary>
    public PipelineBuilder Use<TMiddleware>() where TMiddleware : IBusMiddleware
    {
        _components.Add(next => context =>
        {
            var middleware = context.Services.GetRequiredService<TMiddleware>();
            return Timed(next, context, typeof(TMiddleware).Name, middleware);
        });
        return this;
    }

    /// <summary>
    /// Ветвление: вложенная цепочка выполняется, только если предикат истинен (идея 8).
    /// Ветка собирается на старте, не на каждом сообщении.
    /// </summary>
    public PipelineBuilder UseWhen(Func<ConsumeContext, bool> predicate, Action<PipelineBuilder> branch)
    {
        _components.Add(next =>
        {
            var branchBuilder = new PipelineBuilder();
            branch(branchBuilder);
            var branchPipeline = branchBuilder.Build(next);

            return context => predicate(context) ? branchPipeline(context) : next(context);
        });
        return this;
    }

    /// <summary>Вставляет шаг в начало цепочки — для инфраструктуры, которая обязана быть внешней.</summary>
    public PipelineBuilder UseFirst(IBusMiddleware middleware)
    {
        var name = middleware.GetType().Name;
        _components.Insert(0, next => context => Timed(next, context, name, middleware));
        return this;
    }

    public BusDelegate Build(BusDelegate terminal)
    {
        var pipeline = terminal;

        // Собираем с конца: последний зарегистрированный оказывается ближе всех к терминалу.
        for (var i = _components.Count - 1; i >= 0; i--)
            pipeline = _components[i](pipeline);

        return pipeline;
    }

    /// <summary>Выполняет middleware и замеряет его время в histogram шагов пайплайна.</summary>
    private static async ValueTask Timed(
        BusDelegate next,
        ConsumeContext context,
        string stepName,
        IBusMiddleware middleware)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await middleware.InvokeAsync(context, next).ConfigureAwait(false);
        }
        finally
        {
            stopwatch.Stop();
            // Контекст может быть null только в unit-тестах пайплайна без реального сообщения.
            var messageType = context?.Message?.GetType().Name ?? "?";
            BusTelemetry.RecordPipelineStep(stepName, messageType, stopwatch.Elapsed.TotalMilliseconds);
        }
    }
}
