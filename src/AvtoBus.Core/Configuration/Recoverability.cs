namespace AvtoBus.Configuration;

/// <summary>Класс ошибки — определяет, как с ней поступить.</summary>
public enum RetryClass
{
    /// <summary>Повторить немедленно, в памяти: сетевой глитч, дедлок.</summary>
    Immediate,

    /// <summary>Повторить с задержкой: недоступен внешний сервис.</summary>
    Transient,

    /// <summary>Повторять бессмысленно: невалидные данные, нарушенный контракт.</summary>
    Permanent,
}

/// <summary>Что делать с сообщением, исчерпавшим все попытки.</summary>
public enum FailureAction
{
    /// <summary>В error-очередь, для разбора и реплея.</summary>
    MoveToErrorQueue,

    /// <summary>Выбросить: сообщение не имеет ценности.</summary>
    Discard,
}

/// <summary>Стратегия расчёта задержки между попытками.</summary>
public abstract class Backoff
{
    public abstract TimeSpan Delay(int attempt, Random random);

    public static Backoff Fixed(TimeSpan delay) => new FixedBackoff(delay);

    public static Backoff Linear(TimeSpan step, TimeSpan? cap = null) => new LinearBackoff(step, cap);

    /// <summary>
    /// Экспоненциальный бэкофф с decorrelated jitter — лучшая защита от thundering herd (идея 187).
    /// </summary>
    public static Backoff Exponential(TimeSpan @base, TimeSpan? cap = null, bool jitter = true)
        => new ExponentialBackoff(@base, cap ?? TimeSpan.FromMinutes(5), jitter);

    private sealed class FixedBackoff(TimeSpan delay) : Backoff
    {
        public override TimeSpan Delay(int attempt, Random random) => delay;
    }

    private sealed class LinearBackoff(TimeSpan step, TimeSpan? cap) : Backoff
    {
        public override TimeSpan Delay(int attempt, Random random)
        {
            var delay = step * attempt;
            return cap is { } max && delay > max ? max : delay;
        }
    }

    private sealed class ExponentialBackoff(TimeSpan @base, TimeSpan cap, bool jitter) : Backoff
    {
        public override TimeSpan Delay(int attempt, Random random)
        {
            var exponent = Math.Min(attempt, 30);
            // Avoid TimeSpan overflow: clamp before multiply
            var rawMs = @base.TotalMilliseconds * Math.Pow(2, exponent - 1);
            if (rawMs > cap.TotalMilliseconds)
                rawMs = cap.TotalMilliseconds;
            var raw = TimeSpan.FromMilliseconds(rawMs);

            if (!jitter)
                return raw;

            var lower = @base.TotalMilliseconds;
            var upper = rawMs;
            if (upper <= lower)
                return @base;

            return TimeSpan.FromMilliseconds(random.NextDouble() * (upper - lower) + lower);
        }
    }
}

/// <summary>
/// Политика восстановления после сбоев: немедленные ретраи, отложенные ретраи, финальное решение.
/// </summary>
public sealed class RecoverabilitySettings
{
    private readonly List<(Type Exception, RetryClass Class)> _exceptionMap = [];

    /// <summary>Число мгновенных повторов в памяти, без возврата в брокер.</summary>
    public int ImmediateRetryCount { get; private set; } = 3;

    /// <summary>Число отложенных повторов через retry-очереди.</summary>
    public int DelayedRetryCount { get; private set; } = 3;

    public Backoff DelayedBackoff { get; private set; } = Backoff.Exponential(TimeSpan.FromSeconds(5));

    public FailureAction OnFailureAction { get; private set; } = FailureAction.MoveToErrorQueue;

    /// <summary>Доля попыток-ретраев от общего трафика, выше которой ретраи отключаются (идея 162).</summary>
    public double RetryBudget { get; private set; } = 0.2;

    /// <summary>Общее число попыток обработки одного сообщения.</summary>
    public int TotalAttempts => 1 + ImmediateRetryCount + DelayedRetryCount;

    public RecoverabilitySettings ImmediateRetries(int count)
    {
        ImmediateRetryCount = Guard(count);
        return this;
    }

    public RecoverabilitySettings DelayedRetries(int count, Backoff? backoff = null)
    {
        DelayedRetryCount = Guard(count);
        if (backoff is not null)
            DelayedBackoff = backoff;
        return this;
    }

    public RecoverabilitySettings OnFailure(FailureAction action)
    {
        OnFailureAction = action;
        return this;
    }

    /// <summary>Ограничивает долю ретраев в трафике — защита от retry-штормов (идея 162).</summary>
    public RecoverabilitySettings WithRetryBudget(double fraction)
    {
        if (fraction is < 0 or > 1)
            throw new ArgumentOutOfRangeException(nameof(fraction), fraction, "Бюджет ретраев — доля от 0 до 1.");

        RetryBudget = fraction;
        return this;
    }

    /// <summary>Привязывает тип исключения к классу ошибки (идея 161).</summary>
    public RecoverabilitySettings MapException<TException>(RetryClass retryClass) where TException : Exception
    {
        _exceptionMap.Add((typeof(TException), retryClass));
        return this;
    }

    public RecoverabilitySettings MapException<TException>(FailureAction action) where TException : Exception
        => MapException<TException>(action is FailureAction.Discard ? RetryClass.Permanent : RetryClass.Transient);

    /// <summary>
    /// Классифицирует исключение. Наиболее специфичное правило выигрывает: ищем по точному
    /// совпадению типа, затем по присваиваемости.
    /// </summary>
    public RetryClass Classify(Exception exception)
    {
        // Явный бизнес-отказ ретраить нельзя ни при каких настройках.
        if (exception is MessageRejectedException)
            return RetryClass.Permanent;

        // Авторизация не станет успешной от повторов — principal не изменится (идея 453).
        if (exception is AvtoBus.Pipeline.UnauthorizedMessageException)
            return RetryClass.Permanent;

        if (exception is OperationCanceledException)
            return RetryClass.Transient;

        var type = exception.GetType();

        foreach (var (mapped, retryClass) in _exceptionMap)
        {
            if (mapped == type)
                return retryClass;
        }

        foreach (var (mapped, retryClass) in _exceptionMap)
        {
            if (mapped.IsAssignableFrom(type))
                return retryClass;
        }

        return RetryClass.Transient;
    }

    private static int Guard(int count) => count >= 0
        ? count
        : throw new ArgumentOutOfRangeException(nameof(count), count, "Число ретраев не может быть отрицательным.");
}
