using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Testing;

/// <summary>
/// Вся шина в памяти для тестов (идея 316). Поднимается за одну строку,
/// даёт доступ к записанным сообщениям и умеет дожидаться обработки без Thread.Sleep.
/// </summary>
public sealed class AvtoBusTestHarness : IAsyncDisposable
{
    private readonly IHost _host;

    private AvtoBusTestHarness(IHost host, BusRecorder recorder, FaultInjector faults)
    {
        _host = host;
        Recorder = recorder;
        Faults = faults;
    }

    public IBus Bus => _host.Services.GetRequiredService<IBus>();

    public IServiceProvider Services => _host.Services;

    public BusRecorder Recorder { get; }

    /// <summary>Управляемая инжекция сбоев для проверки ретраев и компенсаций.</summary>
    public FaultInjector Faults { get; }

    public InMemoryTransport Transport => _host.Services.GetRequiredService<InMemoryTransport>();

    /// <summary>Сообщения, обработанные шиной.</summary>
    public IReadOnlyCollection<RecordedMessage> Consumed => Recorder.Consumed;

    /// <summary>Каскадные сообщения, отправленные хендлерами.</summary>
    public IReadOnlyCollection<RecordedMessage> Published => Recorder.Published;

    public IReadOnlyCollection<RecordedFault> Faulted => Recorder.Faults;

    /// <summary>Поднимает шину и дожидается старта консьюмеров.</summary>
    public static async Task<AvtoBusTestHarness> StartAsync(
        Action<BusConfigurator> configure,
        Action<IServiceCollection>? configureServices = null,
        TimeProvider? timeProvider = null)
    {
        var recorder = new BusRecorder();
        var faults = new FaultInjector();

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.Services.AddSingleton(recorder);
        builder.Services.AddSingleton(faults);

        if (timeProvider is not null)
            builder.Services.AddSingleton(timeProvider);

        configureServices?.Invoke(builder.Services);

        builder.Services.AddAvtoBus(bus =>
        {
            bus.UseInMemory();

            // Recorder ставится первым: он должен увидеть сообщение до любого пользовательского шага.
            bus.Pipeline(p => p.Use(new RecordingMiddleware(recorder, faults)));

            configure(bus);
        });

        var host = builder.Build();
        await host.StartAsync().ConfigureAwait(false);

        // Даём консьюмерам подписаться, иначе первые сообщения уйдут в топик без подписчиков.
        await WaitForConsumersAsync(host).ConfigureAwait(false);

        return new AvtoBusTestHarness(host, recorder, faults);
    }

    private static async Task WaitForConsumersAsync(IHost host)
    {
        var consumerHost = host.Services.GetRequiredService<ConsumerHost>();

        // Подписки регистрируются в ExecuteAsync; ждём, пока ранеры появятся.
        for (var i = 0; i < 200 && consumerHost.Runners.Count == 0; i++)
            await Task.Delay(5).ConfigureAwait(false);

        // Ранеры созданы, но подписка на канал происходит при первом MoveNext итератора.
        await Task.Delay(50).ConfigureAwait(false);
    }

    /// <summary>
    /// Дожидается, пока шина обработает <paramref name="count"/> сообщений типа <typeparamref name="T"/>.
    /// Опрос вместо фиксированной паузы: тест не флакает и не ждёт дольше нужного.
    /// </summary>
    public async Task<bool> WaitForConsumedAsync<T>(int count = 1, TimeSpan? timeout = null) where T : class
        => await WaitUntilAsync(() => Recorder.CountConsumed<T>() >= count, timeout).ConfigureAwait(false);

    /// <summary>Дожидается публикации каскадного сообщения указанного типа.</summary>
    public async Task<bool> WaitForPublishedAsync<T>(int count = 1, TimeSpan? timeout = null) where T : class
        => await WaitUntilAsync(() => Recorder.PublishedOf<T>().Count() >= count, timeout).ConfigureAwait(false);

    /// <summary>Общий примитив ожидания условия.</summary>
    public async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var tp = Services.GetService<TimeProvider>() ?? TimeProvider.System;
        var deadline = tp.GetUtcNow() + (timeout ?? TimeSpan.FromSeconds(5));

        while (tp.GetUtcNow() < deadline)
        {
            if (condition())
                return true;

            await Task.Delay(TimeSpan.FromMilliseconds(10), tp).ConfigureAwait(false);
        }

        return condition();
    }

    /// <summary>Глубина очереди — для проверки, что сообщения действительно доехали до DLQ.</summary>
    public int QueueDepth(string queueName)
        => Transport.QueueDepths.TryGetValue(queueName, out var depth) ? depth : 0;

    /// <summary>Дожидается появления сообщений в очереди (например, в <c>*.error</c>).</summary>
    public Task<bool> WaitForQueueDepthAsync(string queueName, int count = 1, TimeSpan? timeout = null)
        => WaitUntilAsync(() => QueueDepth(queueName) >= count, timeout);

    /// <summary>Проталкивает отложенные сообщения — для тестов с виртуальным временем (идея 317).</summary>
    public ValueTask PumpDelayedAsync() => Transport.PumpDelayedAsync();

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        _host.Dispose();
    }
}
