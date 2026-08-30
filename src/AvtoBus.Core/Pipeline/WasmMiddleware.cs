using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Pipeline;

/// <summary>
/// Redpanda WASM Transforms порт: per-record UDF на WASM, hot-reload без деплоя.
/// Tenant загружает `filter.wasm`/`map.wasm` (Rust/Go/JS -> WASM) — шина вызывает его sandbox'ом на каждом сообщении.
/// В .NET реализация через Wasmtime.NET (опционально); стаб — `Func&lt;byte[],byte[]&gt;` для тестов без нативной зависимости.
/// Аналог: Redpanda `rpk transform create`, Pulsar Functions.
/// </summary>
public sealed class WasmOptions
{
    public string WasmPath { get; set; } = "";
    public bool HotReload { get; set; } = true;
    public Func<byte[], byte[]?>? ManagedFallback { get; set; }
}

public interface IWasmTransform
{
    byte[]? Transform(byte[] body, IReadOnlyDictionary<string, string> headers);
}

public sealed class ManagedWasmTransform(WasmOptions opts) : IWasmTransform
{
    public byte[]? Transform(byte[] body, IReadOnlyDictionary<string, string> headers)
    {
        if (opts.ManagedFallback is not null) return opts.ManagedFallback(body);
        // Без Wasmtime — passthrough. Реальный путь: var engine = new Engine(); var module = Module.FromFile(engine, opts.WasmPath);
        return body;
    }
}

public sealed class WasmMiddleware : IBusMiddleware
{
    private readonly IWasmTransform _wasm;
    public WasmMiddleware(IWasmTransform wasm) => _wasm = wasm;
    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var bodyBytes = context.Envelope.Body.ToArray();
        var transformed = _wasm.Transform(bodyBytes, context.Envelope.Headers);
        if (transformed is null)
        {
            context.Skip("wasm-filter");
            return;
        }
        if (transformed.Length != bodyBytes.Length || !transformed.SequenceEqual(bodyBytes))
        {
            context.Items["avtobus.wasm.transformed"] = true;
            context.ReplaceEnvelope(context.Envelope with { Body = transformed });
        }
        await next(context);
    }
}

public static class WasmExtensions
{
    public static BusConfigurator UseWasmTransform(this BusConfigurator bus, string wasmPath, Func<byte[], byte[]?>? fallback = null)
    {
        var opts = new WasmOptions { WasmPath = wasmPath, ManagedFallback = fallback };
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton<IWasmTransform>(new ManagedWasmTransform(opts));
        bus.Pipeline(b => b.Use<WasmMiddleware>());
        return bus;
    }

    public static BusConfigurator UseWasmFunc(this BusConfigurator bus, Func<byte[], byte[]?> transform)
        => bus.UseWasmTransform("", transform);
}
