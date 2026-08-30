using AvtoBus.Configuration;
using AvtoBus.Pipeline;
using AvtoBus.Runtime;

namespace AvtoBus.Tests;

public class BackoffTests
{
    [Fact]
    public void Exponential_backoff_grows_and_respects_the_cap()
    {
        var backoff = Backoff.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), jitter: false);
        var random = new Random(42);

        Assert.Equal(TimeSpan.FromSeconds(1), backoff.Delay(1, random));
        Assert.Equal(TimeSpan.FromSeconds(2), backoff.Delay(2, random));
        Assert.Equal(TimeSpan.FromSeconds(4), backoff.Delay(3, random));

        // Потолок не пробивается даже на большом номере попытки.
        Assert.Equal(TimeSpan.FromSeconds(10), backoff.Delay(20, random));
    }

    [Fact]
    public void Jittered_backoff_stays_within_base_and_cap()
    {
        var backoff = Backoff.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));
        var random = new Random(1);

        for (var attempt = 1; attempt <= 10; attempt++)
        {
            var delay = backoff.Delay(attempt, random);

            Assert.True(delay >= TimeSpan.FromSeconds(1), $"попытка {attempt}: {delay} меньше базы");
            Assert.True(delay <= TimeSpan.FromSeconds(30), $"попытка {attempt}: {delay} больше потолка");
        }
    }

    [Fact]
    public void Huge_attempt_number_does_not_overflow()
    {
        var backoff = Backoff.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5), jitter: false);

        // Без ограничения показателя 2^int.MaxValue дал бы переполнение.
        var delay = backoff.Delay(int.MaxValue, new Random(0));
        Assert.Equal(TimeSpan.FromMinutes(5), delay);
    }

    [Fact]
    public void Linear_backoff_caps_out()
    {
        var backoff = Backoff.Linear(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5));
        var random = new Random(0);

        Assert.Equal(TimeSpan.FromSeconds(2), backoff.Delay(1, random));
        Assert.Equal(TimeSpan.FromSeconds(4), backoff.Delay(2, random));
        Assert.Equal(TimeSpan.FromSeconds(5), backoff.Delay(3, random));
    }
}

public class RecoverabilityClassificationTests
{
    [Fact]
    public void Unmapped_exception_defaults_to_transient()
        => Assert.Equal(RetryClass.Transient, new RecoverabilitySettings().Classify(new Exception("boom")));

    [Fact]
    public void Business_rejection_is_always_permanent()
    {
        // Даже если пользователь ничего не настроил, отклонённое сообщение ретраить нельзя.
        var settings = new RecoverabilitySettings();
        Assert.Equal(RetryClass.Permanent, settings.Classify(new MessageRejectedException("empty")));
    }

    [Fact]
    public void Exact_type_match_wins_over_base_type()
    {
        var settings = new RecoverabilitySettings()
            .MapException<Exception>(RetryClass.Immediate)
            .MapException<ArgumentException>(RetryClass.Permanent);

        Assert.Equal(RetryClass.Permanent, settings.Classify(new ArgumentException()));
        Assert.Equal(RetryClass.Immediate, settings.Classify(new FormatException()));
    }

    [Fact]
    public void Derived_exception_inherits_base_mapping()
    {
        var settings = new RecoverabilitySettings().MapException<ArgumentException>(RetryClass.Permanent);

        // ArgumentNullException наследует ArgumentException.
        Assert.Equal(RetryClass.Permanent, settings.Classify(new ArgumentNullException("param")));
    }

    [Fact]
    public void Retry_budget_rejects_values_outside_zero_to_one()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new RecoverabilitySettings().WithRetryBudget(1.5));
}

public class CircuitBreakerTests
{
    [Fact]
    public void Breaker_opens_after_threshold_consecutive_failures()
    {
        var time = new FakeTimeProvider();
        var breaker = new CircuitBreaker(3, TimeSpan.FromSeconds(30), time);

        breaker.RecordFailure();
        breaker.RecordFailure();
        Assert.Equal(CircuitState.Closed, breaker.State);

        breaker.RecordFailure();
        Assert.Equal(CircuitState.Open, breaker.State);
        Assert.False(breaker.CanProcess());
    }

    [Fact]
    public void Success_resets_the_failure_streak()
    {
        var time = new FakeTimeProvider();
        var breaker = new CircuitBreaker(3, TimeSpan.FromSeconds(30), time);

        breaker.RecordFailure();
        breaker.RecordFailure();
        breaker.RecordSuccess();
        breaker.RecordFailure();
        breaker.RecordFailure();

        // Серия прервана успехом, порог заново не достигнут.
        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public void Breaker_moves_to_half_open_after_the_pause()
    {
        var time = new FakeTimeProvider();
        var breaker = new CircuitBreaker(1, TimeSpan.FromSeconds(30), time);

        breaker.RecordFailure();
        Assert.Equal(CircuitState.Open, breaker.State);

        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(CircuitState.HalfOpen, breaker.State);
        Assert.True(breaker.CanProcess());
    }

    [Fact]
    public void Failure_in_half_open_reopens_immediately()
    {
        var time = new FakeTimeProvider();
        var breaker = new CircuitBreaker(5, TimeSpan.FromSeconds(30), time);

        breaker.RecordFailure();
        breaker.RecordFailure();
        breaker.RecordFailure();
        breaker.RecordFailure();
        breaker.RecordFailure();
        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(CircuitState.HalfOpen, breaker.State);

        // Проба провалилась — размыкаем снова, не дожидаясь нового порога.
        breaker.RecordFailure();
        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public void Disabled_breaker_always_allows_processing()
    {
        var breaker = new CircuitBreaker(0, TimeSpan.FromSeconds(30), new FakeTimeProvider());

        breaker.RecordFailure();
        breaker.RecordFailure();

        Assert.True(breaker.CanProcess());
    }
}

public class InboxDeduplicationTests
{
    [Fact]
    public void First_sighting_is_accepted_and_the_second_is_not()
    {
        var inbox = new InboxDeduplication(TimeSpan.FromMinutes(5), new FakeTimeProvider());
        var messageId = Guid.NewGuid();

        Assert.True(inbox.TryMarkProcessed(messageId, "orders"));
        Assert.False(inbox.TryMarkProcessed(messageId, "orders"));
    }

    [Fact]
    public void Deduplication_is_scoped_per_consumer()
    {
        var inbox = new InboxDeduplication(TimeSpan.FromMinutes(5), new FakeTimeProvider());
        var messageId = Guid.NewGuid();

        // Разные консьюмеры обрабатывают одно событие независимо — это не дубликат.
        Assert.True(inbox.TryMarkProcessed(messageId, "billing"));
        Assert.True(inbox.TryMarkProcessed(messageId, "shipping"));
    }

    [Fact]
    public void Forget_allows_a_retry_to_be_processed()
    {
        var inbox = new InboxDeduplication(TimeSpan.FromMinutes(5), new FakeTimeProvider());
        var messageId = Guid.NewGuid();

        inbox.TryMarkProcessed(messageId, "orders");
        inbox.Forget(messageId, "orders");

        // Обработка провалилась — ретрай не должен считаться дубликатом.
        Assert.True(inbox.TryMarkProcessed(messageId, "orders"));
    }

    [Fact]
    public void Entries_expire_after_the_window()
    {
        var time = new FakeTimeProvider();
        var inbox = new InboxDeduplication(TimeSpan.FromMinutes(5), time);
        var messageId = Guid.NewGuid();

        inbox.TryMarkProcessed(messageId, "orders");
        time.Advance(TimeSpan.FromMinutes(10));

        // Окно прошло: старые записи вычищены, сообщение снова считается новым.
        Assert.True(inbox.TryMarkProcessed(messageId, "orders"));
    }
}

public class PipelineTests
{
    [Fact]
    public async Task Middleware_runs_in_registration_order_around_the_terminal()
    {
        var order = new List<string>();
        var builder = new PipelineBuilder();

        builder.Use(async (context, next) =>
        {
            order.Add("first:in");
            await next(context);
            order.Add("first:out");
        });

        builder.Use(async (context, next) =>
        {
            order.Add("second:in");
            await next(context);
            order.Add("second:out");
        });

        var pipeline = builder.Build(_ =>
        {
            order.Add("terminal");
            return ValueTask.CompletedTask;
        });

        await pipeline(null!);

        Assert.Equal(
            ["first:in", "second:in", "terminal", "second:out", "first:out"],
            order);
    }

    [Fact]
    public async Task Middleware_can_short_circuit_by_not_calling_next()
    {
        var reachedTerminal = false;
        var builder = new PipelineBuilder();

        builder.Use((_, _) => ValueTask.CompletedTask);

        var pipeline = builder.Build(_ =>
        {
            reachedTerminal = true;
            return ValueTask.CompletedTask;
        });

        await pipeline(null!);

        Assert.False(reachedTerminal);
    }
}

public class EnvelopeTests
{
    [Fact]
    public void Expiry_is_measured_from_send_time()
    {
        var sentAt = DateTimeOffset.UtcNow;
        var envelope = NewEnvelope() with { SentAt = sentAt, TimeToLive = TimeSpan.FromSeconds(30) };

        Assert.False(envelope.IsExpired(sentAt.AddSeconds(29)));
        Assert.True(envelope.IsExpired(sentAt.AddSeconds(31)));
    }

    [Fact]
    public void Message_without_ttl_never_expires()
    {
        var envelope = NewEnvelope();
        Assert.False(envelope.IsExpired(DateTimeOffset.UtcNow.AddYears(10)));
    }

    [Fact]
    public void Delivery_is_held_until_deliver_at()
    {
        var now = DateTimeOffset.UtcNow;
        var envelope = NewEnvelope() with { DeliverAt = now.AddMinutes(5) };

        Assert.False(envelope.IsDue(now));
        Assert.True(envelope.IsDue(now.AddMinutes(6)));
    }

    [Fact]
    public void WithHeader_does_not_mutate_the_original()
    {
        var original = NewEnvelope();
        var modified = original.WithHeader("x-test", "value");

        Assert.Empty(original.Headers);
        Assert.Equal("value", modified.Header("x-test"));
    }

    [Fact]
    public void NextAttempt_increments_the_delivery_counter()
        => Assert.Equal(2, NewEnvelope().NextAttempt().DeliveryAttempt);

    private static Envelope NewEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        MessageType = "test.message",
        Body = ReadOnlyMemory<byte>.Empty,
        SentAt = DateTimeOffset.UtcNow,
    };
}

/// <summary>Управляемое время для тестов: позволяет «прыгнуть» вперёд без реальных пауз.</summary>
public sealed class FakeTimeProvider(DateTimeOffset? start = null) : TimeProvider
{
    private DateTimeOffset _now = start ?? new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan delta) => _now += delta;
}
