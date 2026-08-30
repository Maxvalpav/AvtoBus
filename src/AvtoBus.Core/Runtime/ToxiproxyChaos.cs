using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Runtime;

/// <summary>
/// Toxiproxy / ChaosMesh порт (Go/Infra): транспорт-хаос на TCP — latency, down, timeout, duplicate.
/// Декорирует любой `ITransport` и инжектит `Toxic` перед `SendAsync`/`ReceiveAsync`.
/// Аналог: Toxiproxy `toxics: [latency, slow_close, timeout]`, ChaosMesh `NetworkChaos`.
/// Используется только в тестах: `bus.UseToxiproxy(a=>a.Latency(100).Down(0.05))`.
/// </summary>
public sealed class ToxiproxyOptions
{
    public TimeSpan Latency { get; set; } = TimeSpan.Zero;
    public double DownProbability { get; set; } = 0;
    public double DuplicateProbability { get; set; } = 0;
    public TimeSpan Timeout { get; set; } = TimeSpan.Zero;
}

public sealed class ToxiproxyTransport(ITransport inner, ToxiproxyOptions opts) : ITransport
{
    public string Name => inner.Name + "+toxic";
    public ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        if (opts.Latency > TimeSpan.Zero) Thread.Sleep(opts.Latency);
        if (opts.DownProbability > 0 && Random.Shared.NextDouble() < opts.DownProbability)
            throw new IOException("Toxiproxy down toxic");
        if (opts.Timeout > TimeSpan.Zero) ct = new CancellationTokenSource(opts.Timeout).Token;
        var task = inner.SendAsync(envelope, destination, ct);
        if (opts.DuplicateProbability > 0 && Random.Shared.NextDouble() < opts.DuplicateProbability)
            _ = inner.SendAsync(envelope, destination, CancellationToken.None);
        return task;
    }
    public IAsyncEnumerable<ITransportMessage> ReceiveAsync(TransportSubscription sub, CancellationToken ct = default) => inner.ReceiveAsync(sub, ct);
    public ValueTask ProvisionAsync(IReadOnlyCollection<TransportDestination> d, CancellationToken ct = default) => inner.ProvisionAsync(d, ct);
    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

public sealed class ToxiproxyBuilder
{
    private readonly ToxiproxyOptions _o = new();
    public ToxiproxyBuilder Latency(TimeSpan d) { _o.Latency = d; return this; }
    public ToxiproxyBuilder Down(double p) { _o.DownProbability = p; return this; }
    public ToxiproxyBuilder Duplicate(double p) { _o.DuplicateProbability = p; return this; }
    public ToxiproxyBuilder Timeout(TimeSpan t) { _o.Timeout = t; return this; }
    internal ToxiproxyOptions Build() => _o;
}

public static class ToxiproxyExtensions
{
    public static BusConfigurator UseToxiproxy(this BusConfigurator bus, Action<ToxiproxyBuilder> configure)
    {
        var b = new ToxiproxyBuilder();
        configure(b);
        var opts = b.Build();
        // Оборачиваем уже зарегистрированные транспорты
        var descriptors = bus.Services.Where(s => s.ServiceType == typeof(ITransport)).ToList();
        foreach (var d in descriptors)
        {
            bus.Services.AddSingleton<ITransport>(sp =>
            {
                var inner = (ITransport)(d.ImplementationInstance ?? d.ImplementationFactory!.Invoke(sp));
                return new ToxiproxyTransport(inner, opts);
            });
        }
        return bus;
    }
}
