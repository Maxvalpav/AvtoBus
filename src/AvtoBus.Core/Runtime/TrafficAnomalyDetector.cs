using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using AvtoBus.Observability;
using AvtoBus.Pipeline;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Runtime;

/// <summary>Зафиксированная аномалия частоты типа сообщений (идея 314).</summary>
public readonly record struct TrafficAnomaly(
    string MessageType,
    string Direction,
    long Count,
    double Baseline,
    double Ratio);

/// <summary>
/// Аномалия-детектор частоты событий (идея 314): скользящее окно считает сообщения каждого
/// типа, завершённое окно сравнивается со средним предыдущих — рост/падение в N раз порождает
/// <see cref="TrafficAnomaly"/>. Простая статистика, без ML и демонов: всё считается в Record.
/// </summary>
public sealed class TrafficAnomalyDetector
{
    private readonly ConcurrentDictionary<string, TypeCounters> _byType = new(StringComparer.Ordinal);

    /// <summary>Во сколько раз частота должна измениться, чтобы считаться аномалией.</summary>
    public double Threshold { get; }

    private readonly TimeSpan _window;
    private readonly int _historySlots;
    private readonly TimeProvider _time;
    private readonly ILogger<TrafficAnomalyDetector> _logger;

    /// <summary>Все зафиксированные аномалии за время жизни детектора — для тестов и дашборда.</summary>
    public ConcurrentQueue<TrafficAnomaly> Anomalies { get; } = new();

    public TrafficAnomalyDetector(
        double threshold = 10,
        TimeSpan? window = null,
        int historySlots = 12,
        TimeProvider? time = null,
        ILogger<TrafficAnomalyDetector>? logger = null)
    {
        Threshold = threshold;
        _window = window ?? TimeSpan.FromMinutes(1);
        _historySlots = Math.Max(1, historySlots);
        _time = time ?? TimeProvider.System;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<TrafficAnomalyDetector>.Instance;
    }

    /// <summary>Учитывает одно сообщение типа <paramref name="messageType"/> (вызывается из middleware).</summary>
    public void Record(string messageType)
        => _byType.GetOrAdd(messageType, static _ => new TypeCounters()).Record(this, messageType);

    /// <summary>Диагностика: текущий счёт окна и среднее предыдущих — для мониторов.</summary>
    public (long Current, double Baseline) Inspect(string messageType)
        => _byType.TryGetValue(messageType, out var counters)
            ? (counters.Current, counters.Baseline)
            : (0, 0);

    private void Detected(string messageType, string direction, long count, double baseline)
    {
        var ratio = baseline > 0 ? count / baseline : double.PositiveInfinity;
        var anomaly = new TrafficAnomaly(messageType, direction, count, baseline, ratio);
        Anomalies.Enqueue(anomaly);

        AvtoBusEventSource.Log.TrafficAnomaly(messageType, direction, count, ratio);
        _logger.LogWarning(
            "Аномалия трафика {Direction} по типу {MessageType}: {Count} против средних {Baseline:0.##} (x{Ratio:0.#})",
            direction,
            messageType,
            count,
            baseline,
            ratio);
    }

    private void Evaluate(string messageType, long completed, double baseline)
    {
        if (completed >= baseline * Threshold)
            Detected(messageType, "spike", completed, baseline);
        else if (baseline >= Threshold && completed <= baseline / Threshold)
            Detected(messageType, "drop", completed, baseline);
    }

    private sealed class TypeCounters
    {
        private readonly Queue<long> _history = new();
        private DateTimeOffset _windowStarted;
        private long _current;
        private readonly Lock _gate = new();

        public long Current
        {
            get { lock (_gate) return _current; }
        }

        public double Baseline
        {
            get { lock (_gate) return _history.Count > 0 ? _history.Average() : 0; }
        }

        public void Record(TrafficAnomalyDetector owner, string messageType)
        {
            var now = owner._time.GetUtcNow();
            long completed = 0;
            double baseline = 0;
            bool windowClosed = false;

            lock (_gate)
            {
                if (_windowStarted != default && now - _windowStarted >= owner._window)
                {
                    completed = _current;
                    _current = 0;
                    _windowStarted = now;
                    baseline = _history.Count > 0 ? _history.Average() : 0;
                    if (_history.Count >= owner._historySlots)
                        _history.Dequeue();
                    _history.Enqueue(completed);
                    windowClosed = baseline > 0;
                }
                else if (_windowStarted == default)
                {
                    _windowStarted = now;
                }

                _current++;
            }

            if (windowClosed)
                owner.Evaluate(messageType, completed, baseline);
        }
    }
}

/// <summary>
/// Middleware, кормящий <see cref="TrafficAnomalyDetector"/> типами проходящих сообщений.
/// Должен стоять в начале цепочки, чтобы видеть каждую обработку (идея 314).
/// </summary>
public sealed class TrafficAnomalyMiddleware(TrafficAnomalyDetector detector) : IBusMiddleware
{
    public ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        detector.Record(context.Envelope.MessageType);
        return next(context);
    }
}
