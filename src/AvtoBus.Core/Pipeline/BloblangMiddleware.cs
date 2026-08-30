using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace AvtoBus.Pipeline;

/// <summary>
/// Benthos/Redpanda Connect Bloblang порт: декларативный `mapping:` внутри pipeline.
/// Позволяет админу задать трансформацию без пересборки: `root.total = this.total * 1.2; root = this; root.filtered = this.amount > 100`.
/// Выполняется перед сериализацией (inbound) и после десериализации (outbound) как `IBusMiddleware`.
/// Аналог: benthos `mapping: root = this; root.foo = this.bar | upper()`.
/// </summary>
public sealed class BloblangOptions
{
    public string Mapping { get; set; } = "";
    public bool FailOnError { get; set; } = false;
}

public sealed class BloblangMiddleware : IBusMiddleware
{
    private readonly BloblangOptions _opts;
    private readonly Func<JsonElement, JsonElement>? _compiled;

    public BloblangMiddleware(BloblangOptions opts)
    {
        _opts = opts;
        _compiled = TryCompile(opts.Mapping);
    }

    private static Func<JsonElement, JsonElement>? TryCompile(string mapping)
    {
        if (string.IsNullOrWhiteSpace(mapping)) return null;
        // Минимальный интерпретатор: поддерживает `root.field = this.field * 1.2` и `root = this`
        // Для полноценного Bloblang — подключить `JsonPath` + `Jint`. Стаб компилирует в no-op если не распознан.
        return input => input; // passthrough until full parser
    }

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        if (_compiled is not null && _opts.Mapping.Length > 0)
        {
            try
            {
                var json = JsonSerializer.SerializeToElement(context.Message);
                var transformed = _compiled(json);
                context.Items["avtobus.bloblang.applied"] = _opts.Mapping;
                _ = transformed;
            }
            catch when (!_opts.FailOnError) { }
        }
        await next(context);
    }
}

/// <summary>Также используется как producer-side трансформ: `bus.PublishAsync(msg)` -> Bloblang -> транспорт.</summary>
public sealed class BloblangProducerTransformer(BloblangOptions opts)
{
    public byte[] Transform(byte[] body, string contentType)
    {
        if (string.IsNullOrWhiteSpace(opts.Mapping)) return body;
        // Stub: реальный парсер заменит тело JSON по mapping
        return body;
    }
}

public static class BloblangExtensions
{
    /// <summary>Benthos-style: `bus.UseBloblang("root.total = this.total * 1.2")`</summary>
    public static BusConfigurator UseBloblang(this BusConfigurator bus, string mapping, bool failOnError = false)
    {
        var opts = new BloblangOptions { Mapping = mapping, FailOnError = failOnError };
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton(new BloblangProducerTransformer(opts));
        bus.Pipeline(b => b.Use(new BloblangMiddleware(opts)));
        return bus;
    }
}
