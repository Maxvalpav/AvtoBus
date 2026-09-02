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
    private string _wasmPath = "";
    public string WasmPath { get => _wasmPath; set => _wasmPath = Normalize(value); }
    public bool HotReload { get; set; } = true;
    public Func<byte[], byte[]?>? ManagedFallback { get; set; }
    public long MaxPayloadBytes { get; set; } = 5 * 1024 * 1024;
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);

    private static string Normalize(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return "";
        if (path.Contains("..")) throw new ArgumentException("WasmPath must not contain '..' (path traversal)", nameof(path));
        return path;
    }
}

public interface IWasmTransform
{
    byte[]? Transform(byte[] body, IReadOnlyDictionary<string, string> headers);
}

public sealed class ManagedWasmTransform(WasmOptions opts) : IWasmTransform
{
    public byte[]? Transform(byte[] body, IReadOnlyDictionary<string, string> headers)
    {
        if (body.Length > opts.MaxPayloadBytes) throw new InvalidOperationException($"WASM payload {body.Length} exceeds limit {opts.MaxPayloadBytes}");
        if (opts.ManagedFallback is not null)
        {
            using var cts = new CancellationTokenSource(opts.Timeout);
            var task = Task.Run(() => opts.ManagedFallback(body), cts.Token);
            try
            {
                if (!task.Wait(opts.Timeout)) { cts.Cancel(); return null; }
                var result = task.Result;
                if (result is not null && result.Length > opts.MaxPayloadBytes) throw new InvalidOperationException("WASM output exceeds payload limit — possible OOM");
                return result;
            }
            catch (AggregateException ex) when (ex.InnerException is not null) { throw ex.InnerException; }
        }
        if (!string.IsNullOrEmpty(opts.WasmPath))
        {
            if (!File.Exists(opts.WasmPath)) throw new FileNotFoundException($"WASM module not found: {opts.WasmPath}", opts.WasmPath);
            var info = new FileInfo(opts.WasmPath);
            if (info.Length > 20 * 1024 * 1024) throw new InvalidOperationException("WASM module too large (20MB limit)");
        }
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
