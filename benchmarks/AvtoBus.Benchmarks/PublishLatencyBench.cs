using System.Diagnostics;
using System.Threading.Channels;
using AvtoBus.Configuration;
using AvtoBus.InMemory;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Benchmarks;

public sealed record OrderPlaced(Guid OrderId, decimal Amount, string Currency);

/// <summary>
/// Пайплайн Publish → InMemory → Handler (SLO из docs/20-benchmarks.md):
/// латентность p50 &lt; 20 µs, p99 &lt; 100 µs; аллокации на Publish ≤ 1 KB,
/// на Consume ≤ 2 KB.
/// </summary>
[MemoryDiagnoser]
public class PublishLatencyBench : IAsyncDisposable
{
    private IHost _host = null!;
    private IBus _bus = null!;
    private OrderPlaced _message = null!;
    private Channel<OrderPlaced> _roundTrip = null!;

    [GlobalSetup]
    public void Setup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddAvtoBus(bus => bus
            .UseInMemory()
            .Subscribe<OrderPlaced>((msg, _) =>
            {
                if (_roundTrip is not null)
                    _roundTrip.Writer.TryWrite(msg);
                return Task.CompletedTask;
            }));

        _host = builder.Build();
        _host.StartAsync().GetAwaiter().GetResult();
        _bus = _host.Services.GetRequiredService<IBus>();
        _message = new OrderPlaced(Guid.NewGuid(), 100m, "USD");
    }

    [IterationSetup]
    public void IterationSetup() =>
        _roundTrip = Channel.CreateBounded<OrderPlaced>(
            new BoundedChannelOptions(1) { SingleReader = true, SingleWriter = true });

    /// <summary>Полный пайплайн до консьюмера: Publish + ожидание обработки в хендлере.</summary>
    [Benchmark]
    public async Task Publish_InMemory()
    {
        var sw = Stopwatch.StartNew();
        await _bus.PublishAsync(_message);
        await _roundTrip.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        sw.Stop();
        ConsumeLatencyMicros = sw.Elapsed.TotalMicroseconds;
    }

    /// <summary>Только путь публикации: асинхронный dispatch в канал, без ожидания хендлера.</summary>
    [Benchmark]
    public async Task PublishOnly_InMemory()
    {
        await _bus.PublishAsync(_message);
    }

    /// <summary>Сквозная латентность последнего прогона (от Publish до завершения хендлера).</summary>
    public double ConsumeLatencyMicros { get; private set; }

    public async ValueTask DisposeAsync()
    {
        if (_host is not null)
            await _host.StopAsync();
    }
}
