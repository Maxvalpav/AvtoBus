using AvtoBus.Runtime;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>Аномалия-детектор частоты событий (идея 314).</summary>
public class TrafficAnomalyTests
{
    private static (TrafficAnomalyDetector Detector, FakeTimeProvider Time) Create(double threshold = 10)
    {
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var detector = new TrafficAnomalyDetector(
            threshold: threshold,
            window: TimeSpan.FromMinutes(1),
            historySlots: 12,
            time: time);
        return (detector, time);
    }

    [Fact]
    public void Stable_traffic_produces_no_anomalies()
    {
        var (detector, time) = Create();

        // Несколько окон с одинаковой интенсивностью — база выравнивается.
        for (var window = 0; window < 6; window++)
        {
            for (var i = 0; i < 5; i++)
                detector.Record("orders");

            time.Advance(TimeSpan.FromMinutes(1));
        }

        Assert.Empty(detector.Anomalies);
    }

    [Fact]
    public void Spike_of_more_than_threshold_times_is_detected()
    {
        var (detector, time) = Create(threshold: 10);

        // Базовые окна: по 5 сообщений.
        for (var window = 0; window < 3; window++)
        {
            for (var i = 0; i < 5; i++)
                detector.Record("orders");
            time.Advance(TimeSpan.FromMinutes(1));
        }

        // Всплеск: 200 сообщений в одном окне (40x от базы) → spike.
        for (var i = 0; i < 200; i++)
            detector.Record("orders");
        time.Advance(TimeSpan.FromMinutes(1));

        // Первое сообщение следующего окна закрывает всплеск и запускает оценку.
        detector.Record("orders");

        var spikes = detector.Anomalies.Where(a => a.Direction == "spike").ToList();
        Assert.Single(spikes);
        Assert.Equal(200, spikes[0].Count);
        Assert.Equal("orders", spikes[0].MessageType);
    }

    [Fact]
    public void Drop_of_more_than_threshold_times_is_detected()
    {
        var (detector, time) = Create(threshold: 10);

        // Базовые окна: по 50 сообщений.
        for (var window = 0; window < 3; window++)
        {
            for (var i = 0; i < 50; i++)
                detector.Record("orders");
            time.Advance(TimeSpan.FromMinutes(1));
        }

        // Провал: 1 сообщение (50x меньше базы) → drop.
        detector.Record("orders");
        time.Advance(TimeSpan.FromMinutes(1));

        // Первое сообщение следующего окна закрывает пустой промежуток и запускает оценку.
        detector.Record("orders");

        var drops = detector.Anomalies.Where(a => a.Direction == "drop").ToList();
        Assert.Single(drops);
        Assert.Equal(1, drops[0].Count);
    }
}
