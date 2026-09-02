namespace AvtoBus.Runtime;

public enum CircuitState
{
    /// <summary>Всё в порядке: сообщения обрабатываются.</summary>
    Closed,

    /// <summary>Цепь разомкнута: консьюмер на паузе, сообщения остаются в брокере.</summary>
    Open,

    /// <summary>Пробный режим: пропускаем одно сообщение, чтобы проверить, ожил ли ресурс.</summary>
    HalfOpen,
}

/// <summary>
/// Circuit breaker на консьюмер (идея 163). Размыкает цепь после N ошибок подряд:
/// сообщения не вычитываются и остаются в брокере, а не сгорают в ретраях.
/// </summary>
public sealed class CircuitBreaker(int threshold, TimeSpan duration, TimeProvider time)
{
    private readonly Lock _gate = new();
    private int _consecutiveFailures;
    private DateTimeOffset _openedAt;
    private CircuitState _state = CircuitState.Closed;

    public bool IsEnabled => threshold > 0;

    public CircuitState State
    {
        get
        {
            lock (_gate)
                return Evaluate();
        }
    }

    /// <summary>Детерминированный снапшот без побочного перехода Open→HalfOpen (для health checks).</summary>
    public CircuitState RawState
    {
        get { lock (_gate) return _state; }
    }

    /// <summary>Можно ли сейчас обрабатывать сообщение.</summary>
    public bool CanProcess()
    {
        if (!IsEnabled)
            return true;

        lock (_gate)
            return Evaluate() is not CircuitState.Open;
    }

    public void RecordSuccess()
    {
        if (!IsEnabled)
            return;

        lock (_gate)
        {
            _consecutiveFailures = 0;
            _state = CircuitState.Closed;
        }
    }

    public void RecordFailure()
    {
        if (!IsEnabled)
            return;

        lock (_gate)
        {
            _consecutiveFailures++;

            // Провал в half-open означает, что ресурс всё ещё лежит: размыкаем заново.
            if (_state is CircuitState.HalfOpen || _consecutiveFailures >= threshold)
            {
                _state = CircuitState.Open;
                _openedAt = time.GetUtcNow();
            }
        }
    }

    /// <summary>Сколько ждать до следующей пробы; <see cref="TimeSpan.Zero"/> — можно пробовать сейчас.</summary>
    public TimeSpan RetryAfter()
    {
        lock (_gate)
        {
            if (Evaluate() is not CircuitState.Open)
                return TimeSpan.Zero;

            var elapsed = time.GetUtcNow() - _openedAt;
            return elapsed >= duration ? TimeSpan.Zero : duration - elapsed;
        }
    }

    /// <summary>Переводит Open в HalfOpen по истечении паузы. Вызывается под захваченным замком.</summary>
    private CircuitState Evaluate()
    {
        if (_state is CircuitState.Open && time.GetUtcNow() - _openedAt >= duration)
            _state = CircuitState.HalfOpen;

        return _state;
    }
}
