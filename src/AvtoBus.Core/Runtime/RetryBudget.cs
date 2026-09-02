namespace AvtoBus.Runtime;

/// <summary>
/// Ретрай-бюджет (Linkerd-style, идея 162): ретраи не должны съедать больше заданной доли
/// трафика. Пока выборка мала, ретраи разрешены — статистика не репрезентативна.
/// Превысили долю — ретраи отключаются, сообщения уходят в финальное решение.
/// </summary>
public sealed class RetryBudget(double fraction, TimeSpan window, TimeProvider time, int minSample = 10)
{
    private readonly Lock _gate = new();
    private DateTimeOffset _windowStart = time.GetUtcNow();
    private long _total;
    private long _retries;

    /// <summary>Бюджет <c>1.0</c> не ограничивает ничего — ретраи всегда разрешены.</summary>
    public bool IsEnabled => fraction < 1.0;

    public bool CanRetry()
    {
        if (!IsEnabled)
            return true;

        lock (_gate)
        {
            RotateIfNeeded();

            // Малый объём выборки — не блокируем: это может быть обычная волна ретраев
            // одного сообщения при простаивающем в остальном консьюмере.
            if (_total < minSample)
                return true;

            return (double)_retries / _total <= fraction;
        }
    }

    /// <summary>Фиксирует исход обработки: ретрай или нет.</summary>
    public void Record(bool isRetry)
    {
        lock (_gate)
        {
            RotateIfNeeded();
            _total++;
            if (isRetry)
                _retries++;
        }
    }

    private void RotateIfNeeded()
    {
        var now = time.GetUtcNow();
        if (now < _windowStart) { _windowStart = now; _total = 0; _retries = 0; return; }
        if (now - _windowStart >= window)
        {
            _windowStart = now;
            _total = 0;
            _retries = 0;
        }
    }
}
