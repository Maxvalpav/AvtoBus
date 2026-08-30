using BenchmarkDotNet.Attributes;

namespace AvtoBus.Benchmarks;

/// <summary>Дополнительно к PublishLatencyBench: изоляция consume-пути + outbox.</summary>
[MemoryDiagnoser]
public class ConsumeLatencyBench : PublishLatencyBench
{
    [Benchmark]
    public async Task Consume_Only()
    {
        // Используем уже настроенный _bus/_roundTrip из базы
        await Publish_InMemory();
    }
}
