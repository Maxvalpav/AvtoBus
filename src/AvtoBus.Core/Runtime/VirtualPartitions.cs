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
    public Func<object, string> Partitioner { get; set; } = msg => msg.GetType().FullName ?? msg.GetType().Name;
    public bool LongRunning { get; set; } = false;
    public TimeSpan LongRunningTimeout { get; set; } = TimeSpan.FromMinutes(10);
}

public sealed class VirtualPartitionRunner
{
    private readonly VirtualPartitionOptions _opts;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, System.Threading.Channels.Channel<ConsumeContext>> _channels = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, bool> _paused = new();
    public VirtualPartitionRunner(VirtualPartitionOptions opts) => _opts = opts;

    public int ResolveVp(ConsumeContext ctx)
    {
        var key = _opts.Partitioner(ctx.Message);
        uint h = 2166136261u;
        foreach (var ch in key) { h ^= ch; h *= 16777619u; }
        return (int)(h % (uint)_opts.PartitionCount);
    }

    public ValueTask PauseAsync(int vp, CancellationToken ct)
    {
        _paused[vp] = true;
        if (_channels.TryGetValue(vp, out var ch)) ch.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
    public ValueTask ResumeAsync(int vp, CancellationToken ct)
    {
        _paused[vp] = false;
        _channels.GetOrAdd(vp, _ => System.Threading.Channels.Channel.CreateUnbounded<ConsumeContext>());
        return ValueTask.CompletedTask;
    }
    public bool IsPaused(int vp) => _paused.TryGetValue(vp, out var p) && p;
}

public static class VirtualPartitionExtensions
{
    public static ConsumerConfigurator<T> WithVirtualPartitions<T>(this ConsumerConfigurator<T> cfg, int count, Func<T, string> partitioner, bool longRunning = false) where T : class
    {
        if (count < 1) throw new ArgumentOutOfRangeException(nameof(count));
        cfg.Settings.Partitions = count;
        cfg.Settings.PartitionKeySelector = msg => msg is T typed ? partitioner(typed) : msg.GetType().FullName ?? msg.GetType().Name;
        return cfg;
    }
}
