using AvtoBus.Configuration;
using AvtoBus.Runtime;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

public class ReliabilityTests
{
    [Fact]
    public async Task Transient_failure_is_retried_until_it_succeeds()
    {
        var attempts = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r.ImmediateRetries(5).DelayedRetries(0))
            .Subscribe<OrderPaid>((_, _) =>
            {
                // Падаем дважды, на третьей попытке отрабатываем успешно.
                if (Interlocked.Increment(ref attempts) <= 2)
                    throw new InvalidOperationException("временный сбой");

                return Task.CompletedTask;
            }));

        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()));

        Assert.True(await harness.WaitUntilAsync(() => Volatile.Read(ref attempts) >= 3));
        Assert.Equal(3, Volatile.Read(ref attempts));
    }

    [Fact]
    public async Task Exhausted_retries_send_message_to_error_queue()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r.ImmediateRetries(2).DelayedRetries(0))
            .Subscribe<OrderPaid>((_, _) => throw new InvalidOperationException("всегда падает")));

        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()));

        // Имя очереди складывается из топика и группы консьюмеров, поэтому ищем по суффиксу:
        // тест проверяет факт попадания в error-очередь, а не конкретную схему именования.
        Assert.True(await harness.WaitUntilAsync(
            () => harness.Transport.QueueDepths
                .Any(q => q.Key.EndsWith(".error", StringComparison.Ordinal) && q.Value > 0),
            TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Dead_lettered_message_carries_rich_error_metadata()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r.ImmediateRetries(1).DelayedRetries(0))
            .Subscribe<OrderPaid>((_, _) => throw new InvalidOperationException("всегда падает")));

        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()));

        Assert.True(await harness.WaitUntilAsync(
            () => harness.Transport.QueueDepths
                .Any(q => q.Key.EndsWith(".error", StringComparison.Ordinal) && q.Value > 0),
            TimeSpan.FromSeconds(10)));

        var errorQueue = harness.Transport.QueueDepths
            .First(q => q.Key.EndsWith(".error", StringComparison.Ordinal) && q.Value > 0)
            .Key;

        var reader = new DlqReader(new TransportRegistry([harness.Transport], "inmemory"));
        var messages = await reader.BrowseAsync(TransportDestination.Queue(errorQueue));

        var message = Assert.Single(messages);
        var envelope = message.Envelope;

        // «Богатые метаданные»: тип исключения, сообщение и стектрейс в заголовках (идея 165).
        Assert.Equal(typeof(InvalidOperationException).FullName, envelope.Header(BusHeaders.ExceptionType));
        Assert.Equal("всегда падает", envelope.Header(BusHeaders.ExceptionMessage));
        Assert.False(string.IsNullOrEmpty(envelope.Header(BusHeaders.ExceptionStackTrace)));

        Assert.False(string.IsNullOrEmpty(envelope.Header(BusHeaders.DeadLetterReason)));
        Assert.False(string.IsNullOrEmpty(envelope.Header(BusHeaders.FailedAt)));
        Assert.False(string.IsNullOrEmpty(envelope.Header(BusHeaders.OriginalDestination)));
        Assert.False(string.IsNullOrEmpty(envelope.Header(BusHeaders.FailedQueue)));
    }

    [Fact]
    public async Task Permanent_exception_skips_retries_entirely()
    {
        var attempts = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r
                .ImmediateRetries(5)
                .MapException<ArgumentException>(RetryClass.Permanent))
            .Subscribe<OrderPaid>((_, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new ArgumentException("невалидные данные");
            }));

        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()));

        await Task.Delay(500);

        // Перманентная ошибка не ретраится: ровно одна попытка.
        Assert.Equal(1, Volatile.Read(ref attempts));
    }

    [Fact]
    public async Task Business_rejection_goes_to_dlq_without_retries()
    {
        var attempts = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r.ImmediateRetries(5))
            .Subscribe<OrderPlaced>(context =>
            {
                Interlocked.Increment(ref attempts);
                context.DeadLetter("пустой заказ");
                return Task.CompletedTask;
            }));

        await harness.Bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), 0m));

        await Task.Delay(500);
        Assert.Equal(1, Volatile.Read(ref attempts));
    }

    [Fact]
    public async Task Rejected_result_is_not_retried()
    {
        var attempts = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .Recoverability(r => r.ImmediateRetries(5))
                .AddConsumer(typeof(RejectingHandlers)),
            services => services.AddSingleton(new AttemptCounter(() => Interlocked.Increment(ref attempts))));

        await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), "cust", 0m));

        await Task.Delay(500);
        Assert.Equal(1, Volatile.Read(ref attempts));
    }

    [Fact]
    public async Task Unknown_contract_lands_in_poison_queue()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.Subscribe<OrderPaid>((_, _) => Task.CompletedTask));

        // Кладём конверт с типом, которого нет в реестре, прямо в очередь консьюмера:
        // разрешить его невозможно, ретраи бессмысленны — прямая дорога в poison.
        var transport = harness.Transport;
        var queue = transport.QueueDepths.Keys.Single(q => q.Contains("order-paid", StringComparison.Ordinal)
                                                           && !q.EndsWith(".error", StringComparison.Ordinal)
                                                           && !q.EndsWith(".poison", StringComparison.Ordinal));

        await transport.SendAsync(
            new Envelope
            {
                MessageId = Guid.NewGuid(),
                MessageType = "unknown.contract.v1",
                Body = "{}"u8.ToArray(),
                SentAt = DateTimeOffset.UtcNow,
            },
            TransportDestination.Queue(queue));

        Assert.True(await harness.WaitForQueueDepthAsync($"{queue}.poison", 1, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Fault_injection_drives_retry_verification()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r.ImmediateRetries(3).DelayedRetries(0))
            .Subscribe<ChargeCard>((_, _) => Task.CompletedTask));

        // Уронить первые две доставки — потом должно пройти.
        harness.Faults.FailNext<ChargeCard>(times: 2);

        await harness.Bus.SendAsync(new ChargeCard(Guid.NewGuid(), 100m));

        Assert.True(await harness.WaitUntilAsync(() => harness.Recorder.CountConsumed<ChargeCard>() >= 3));
        Assert.Equal(2, harness.Recorder.Faults.Count);
    }

    [Fact]
    public async Task Inbox_deduplication_suppresses_repeated_delivery()
    {
        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .UseInboxDeduplication(TimeSpan.FromMinutes(5))
            .Subscribe<OrderPaid>((_, _) =>
            {
                Interlocked.Increment(ref handled);
                return Task.CompletedTask;
            }));

        var messageId = Guid.NewGuid();
        var options = new PublishOptions { MessageId = messageId };

        // Один и тот же MessageId дважды — обработка должна произойти один раз.
        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), options);
        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()), new PublishOptions { MessageId = messageId });

        await Task.Delay(500);
        Assert.Equal(1, Volatile.Read(ref handled));
    }

    [Fact]
    public async Task Expired_message_never_reaches_the_handler()
    {
        var handled = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Subscribe<ShortLived>((_, _) =>
            {
                Interlocked.Increment(ref handled);
                return Task.CompletedTask;
            }));

        // TTL контракта — 1 мс. Задержка заставляет сообщение переждать срок в отложенной
        // очереди: к моменту вычитки оно гарантированно протухло, хендлер не вызывается.
        await harness.Bus.PublishAsync(new ShortLived("тик"), new PublishOptions
        {
            TimeToLive = TimeSpan.FromMilliseconds(1),
            DeliverAt = DateTimeOffset.UtcNow.AddMilliseconds(400),
        });

        await Task.Delay(400);
        Assert.Equal(0, Volatile.Read(ref handled));
    }

    [Fact]
    public async Task Crash_after_commit_before_ack_duplicates_effect_not_loss()
    {
        var commits = 0;

        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r.ImmediateRetries(2).DelayedRetries(0))
            .Subscribe<OrderPaid>((_, _) =>
            {
                // «Коммит» (внешний эффект) уже выполнен, но ack так и не отправлен —
                // имитация краша между commit и ack: at-least-once превращает это в дубль.
                Interlocked.Increment(ref commits);
                throw new InvalidOperationException("crash после commit, до ack");
            }));

        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()));

        // Дубль: эффект выполнен на каждой повторной доставке (1 исходная + 2 ретрая).
        Assert.True(await harness.WaitUntilAsync(() => Volatile.Read(ref commits) >= 3, TimeSpan.FromSeconds(10)));
        Assert.Equal(3, Volatile.Read(ref commits));

        // Не потеряно: исчерпав все попытки, сообщение ушло в error-очередь с метаданными.
        Assert.True(await harness.WaitUntilAsync(
            () => harness.Transport.QueueDepths
                .Any(q => q.Key.EndsWith(".error", StringComparison.Ordinal) && q.Value > 0),
            TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Skipped_outcome_is_not_treated_as_failure()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(bus => bus
            .Subscribe<OrderPaid>(context =>
            {
                context.Skip("дубликат по бизнес-ключу");
                return Task.CompletedTask;
            }));

        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()));

        Assert.True(await harness.WaitForConsumedAsync<OrderPaid>());
        await Task.Delay(300);

        // Skip — не ошибка: ретраев быть не должно.
        Assert.Equal(1, harness.Recorder.CountConsumed<OrderPaid>());
        Assert.Empty(harness.Recorder.Faults);
    }
}

public sealed record AttemptCounter(Action Increment);

public static class RejectingHandlers
{
    public static Result<OrderPlaced> Handle(PlaceOrder command, AttemptCounter counter)
    {
        counter.Increment();

        // Бизнес-отказ: ретраить бессмысленно, это корректный исход.
        return Result<OrderPlaced>.Reject("empty_order");
    }
}
