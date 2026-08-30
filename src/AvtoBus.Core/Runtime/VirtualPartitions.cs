using AvtoBus.Configuration;

namespace AvtoBus.Runtime;

/// <summary>
/// Karafka Pro Virtual Partitions + Long Running Jobs порт (Ruby).
/// Virtual Partitions: 1 Kafka партиция -> N виртуальных потоков по `partitioner(key) % VpCount`, ordered внутри VP.
/// LongRunningJobs: хендлер может `Pause` партицию, работать минутами, `Resume` — не блокируя poll loop.
/// Аналог: Karafka `virtual_partitions: { partitioner: ->(msg){ msg.key } }`, `long_running_job: true`.
/// </summary>
public sealed class VirtualPartitionOptions
{
    public int PartitionCount { get; set; } = 4;
    public Func<object, string> Partitioner { get; set; } = msg => msg.GetHashCode().ToString();
    public bool LongRunning { get; set; } = false;
    public TimeSpan LongRunningTimeout { get; set; } = TimeSpan.FromMinutes(10);
}

public sealed class VirtualPartitionRunner
{
    private readonly VirtualPartitionOptions _opts;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, System.Threading.Channels.Channel<ConsumeContext>> _channels = new();
    public VirtualPartitionRunner(VirtualPartitionOptions opts) => _opts = opts;

    public int ResolveVp(ConsumeContext ctx)
    {
        var key = _opts.Partitioner(ctx.Message);
        return Math.Abs(key.GetHashCode()) % _opts.PartitionCount;
    }

    public ValueTask PauseAsync(int vp, CancellationToken ct) => ValueTask.CompletedTask; // stub: реальный — pause Kafka partition
    public ValueTask ResumeAsync(int vp, CancellationToken ct) => ValueTask.CompletedTask;
}

public static class VirtualPartitionExtensions
{
    public static ConsumerConfigurator<T> WithVirtualPartitions<T>(this ConsumerConfigurator<T> cfg, int count, Func<T, string> partitioner, bool longRunning = false) where T : class
    {
        cfg.Settings.Partitions = count;
        cfg.Settings.PartitionKeySelector = msg => partitioner((T)msg);
        // Флаг LRJ — в Items для ConsumerHost
        return cfg;
    }
}
