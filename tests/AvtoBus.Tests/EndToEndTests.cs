using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>Сквозные сценарии: сообщение реально проходит весь путь через шину.</summary>
public class EndToEndTests
{
    [Fact]
    public async Task Interface_consumer_receives_published_event()
    {
        var received = new TaskCompletionSource<OrderPlaced>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer<OrderPlacedConsumer>(),
            services => services.AddSingleton(received));

        var expected = new OrderPlaced(Guid.NewGuid(), 199.99m);
        await harness.Bus.PublishAsync(expected);

        var actual = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Method_handler_is_discovered_by_convention()
    {
        var log = new ConcurrentBag<Guid>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer(typeof(StaticMethodHandlers)),
            services => services.AddSingleton(log));

        StaticMethodHandlers.Log = log;

        await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), "cust-1", 10m));

        Assert.True(await harness.WaitForConsumedAsync<PlaceOrder>());
        Assert.Single(log);
    }

    [Fact]
    public async Task Lambda_subscription_handles_message()
    {
        var handled = new TaskCompletionSource<OrderPaid>();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
            bus.Subscribe<OrderPaid>((message, _) =>
            {
                handled.TrySetResult(message);
                return Task.CompletedTask;
            }));

        var paid = new OrderPaid(Guid.NewGuid());
        await harness.Bus.PublishAsync(paid);

        Assert.Equal(paid, await handled.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Returned_value_is_published_as_cascade()
    {
        var receipts = new ConcurrentBag<ReceiptRequested>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .AddConsumer(typeof(CascadeHandlers))
                .Subscribe<ReceiptRequested>((message, _) =>
                {
                    receipts.Add(message);
                    return Task.CompletedTask;
                }));

        var orderId = Guid.NewGuid();
        await harness.Bus.PublishAsync(new OrderPaid(orderId));

        Assert.True(await harness.WaitForConsumedAsync<ReceiptRequested>());
        Assert.Equal(orderId, Assert.Single(receipts).OrderId);
    }

    [Fact]
    public async Task Tuple_return_publishes_every_element()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer(typeof(TupleCascadeHandlers)));

        await harness.Bus.SendAsync(new ChargeCard(Guid.NewGuid(), 42m));

        Assert.True(await harness.WaitForPublishedAsync<OrderPaid>());
        Assert.True(await harness.WaitForPublishedAsync<ReceiptRequested>());
    }

    [Fact]
    public async Task OutgoingMessages_builder_dispatches_conditionally()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer(typeof(ConditionalHandlers)));

        await harness.Bus.PublishAsync(new OrderPlaced(Guid.NewGuid(), 5_000m));

        // Крупный заказ должен породить команду списания.
        Assert.True(await harness.WaitForPublishedAsync<ChargeCard>());
    }

    [Fact]
    public async Task Correlation_and_causation_form_a_causality_tree()
    {
        // Подписчик на каскад нужен, чтобы его конверт дошёл до recorder-а: у события
        // без подписчиков нет доставки, а значит и записи о consume.
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .AddConsumer(typeof(CascadeHandlers))
                .Subscribe<ReceiptRequested>((_, _) => Task.CompletedTask));

        var orderId = Guid.NewGuid();
        await harness.Bus.PublishAsync(new OrderPaid(orderId));

        Assert.True(await harness.WaitForConsumedAsync<ReceiptRequested>());

        var parent = harness.Recorder.Consumed.First(m => m.Message is OrderPaid).Envelope;
        var child = harness.Recorder.Consumed.First(m => m.Message is ReceiptRequested).Envelope;

        // Каскад наследует поток и указывает на прямого родителя.
        Assert.Equal(parent.CorrelationId, child.CorrelationId);
        Assert.Equal(parent.MessageId, child.CausationId);
    }

    [Fact]
    public async Task Polymorphic_subscription_catches_derived_events()
    {
        var caught = new TaskCompletionSource<IOrderEvent>();

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
            bus.Subscribe<IOrderEvent>((message, _) =>
            {
                caught.TrySetResult(message);
                return Task.CompletedTask;
            })
            .AddContract<OrderArchived>());

        await harness.Bus.PublishAsync(new OrderArchived(Guid.NewGuid()));

        var received = await caught.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<OrderArchived>(received);
    }

    [Fact]
    public async Task Request_response_returns_typed_reply()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer(typeof(QuoteHandlers)));

        var quote = await harness.Bus.RequestAsync<GetQuote, QuoteResult>(
            new GetQuote("MSFT"),
            TimeSpan.FromSeconds(10));

        Assert.Equal("MSFT", quote.Symbol);
        Assert.Equal(420m, quote.Price);
    }

    [Fact]
    public async Task Request_times_out_when_nobody_answers()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddContract<GetQuote>().AddContract<QuoteResult>());

        await Assert.ThrowsAsync<TimeoutException>(async () =>
            await harness.Bus.RequestAsync<GetQuote, QuoteResult>(
                new GetQuote("NOBODY"),
                TimeSpan.FromMilliseconds(300)));
    }

    [Fact]
    public async Task Request_timeout_cleans_up_the_waiter_registry()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddContract<GetQuote>().AddContract<QuoteResult>());

        var replies = harness.Services.GetRequiredService<AvtoBus.Runtime.ReplyRouter>();

        await Assert.ThrowsAsync<TimeoutException>(async () =>
            await harness.Bus.RequestAsync<GetQuote, QuoteResult>(
                new GetQuote("NOBODY"),
                TimeSpan.FromMilliseconds(300)));

        // Реестр ожиданий очищен после таймаута: поздний ответ не найдёт ожидающего.
        Assert.Equal(0, replies.PendingCount);
    }

    [Fact]
    public async Task Cancelled_request_cleans_up_the_waiter_registry()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddContract<GetQuote>().AddContract<QuoteResult>());

        var replies = harness.Services.GetRequiredService<AvtoBus.Runtime.ReplyRouter>();

        using var cts = new CancellationTokenSource();
        var request = harness.Bus.RequestAsync<GetQuote, QuoteResult>(
            new GetQuote("LATE"),
            TimeSpan.FromSeconds(30),
            cts.Token);

        // Ожидание зарегистрировано до отправки.
        Assert.Equal(1, replies.PendingCount);

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await request);

        // Отмена тоже освобождает ожидание — утечки нет.
        Assert.Equal(0, replies.PendingCount);
    }

    [Fact]
    public async Task Late_reply_after_timeout_is_acked_and_never_requeued()
    {
        var lateCount = 0L;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "AvtoBus")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "avtobus.reply.late")
                Interlocked.Add(ref lateCount, value);
        });
        listener.Start();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddContract<GetQuote>().AddContract<QuoteResult>();
                bus.AddConsumer<SlowQuoteConsumer>();
            });

        // Таймаут короче, чем время ответа: ответ придёт в reply-очередь уже после истечения.
        await Assert.ThrowsAsync<TimeoutException>(async () =>
            await harness.Bus.RequestAsync<GetQuote, QuoteResult>(
                new GetQuote("LATE"),
                TimeSpan.FromMilliseconds(100)));

        // Поздний ответ подтверждается и фиксируется метрикой — не requeue, не повторная доставка.
        Assert.True(await harness.WaitUntilAsync(() => Volatile.Read(ref lateCount) >= 1, TimeSpan.FromSeconds(5)),
            "Поздний ответ не был подтверждён и зафиксирован метрикой");

        // Повторной доставки нет: ответ обрабатывается ровно один раз.
        await Task.Delay(300);
        Assert.Equal(1, Volatile.Read(ref lateCount));
    }

    [Fact]
    public async Task Scheduled_message_is_not_delivered_before_its_time()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer(typeof(ReminderHandlers)));

        await harness.Bus.ScheduleAsync(
            new SendReminder(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddMilliseconds(400));

        // Сразу после планирования сообщения быть не должно.
        await Task.Delay(100);
        Assert.Equal(0, harness.Recorder.CountConsumed<SendReminder>());

        Assert.True(await harness.WaitForConsumedAsync<SendReminder>(1, TimeSpan.FromSeconds(3)));
    }

    [Fact]
    public async Task Cancelled_schedule_never_fires()
    {
        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer(typeof(ReminderHandlers)));

        var token = await harness.Bus.ScheduleAsync(
            new SendReminder(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddSeconds(1));

        await harness.Bus.CancelScheduledAsync(token);

        await Task.Delay(1_500);
        Assert.Equal(0, harness.Recorder.CountConsumed<SendReminder>());
    }

    [Fact]
    public async Task Reply_routing_is_isolated_per_instance()
    {
        // Два инстанса шины (две «реплики»): у каждого своя уникальная reply-очередь.
        await using var instanceA = await AvtoBusTestHarness.StartAsync(bus =>
            bus.Subscribe<OrderPlaced>((_, _) => Task.CompletedTask));
        await using var instanceB = await AvtoBusTestHarness.StartAsync(bus =>
            bus.Subscribe<OrderPlaced>((_, _) => Task.CompletedTask));

        var routerA = instanceA.Services.GetRequiredService<AvtoBus.Runtime.ReplyRouter>();
        var routerB = instanceB.Services.GetRequiredService<AvtoBus.Runtime.ReplyRouter>();

        // Reply endpoint уникален на инстанс — ответ не может быть перехвачен другой репликой.
        Assert.NotEqual(routerA.ReplyAddress, routerB.ReplyAddress);

        // Запрос зарегистрирован только у A. B его не знает и завершить не может;
        // ответ доставляется владельцу запроса (A), а не другой реплике.
        var requestId = Guid.NewGuid();
        var pending = routerA.RegisterAsync(requestId, typeof(OrderPaid), TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.False(routerB.TryComplete(requestId, new OrderPaid(Guid.NewGuid())));
        Assert.False(routerB.TryFail(requestId, new InvalidOperationException("not ours")));
        Assert.True(routerA.TryComplete(requestId, new OrderPaid(Guid.NewGuid())));

        Assert.IsType<OrderPaid>(await pending);
    }
}

// ---- Хендлеры сценариев ------------------------------------------------

public sealed class OrderPlacedConsumer(TaskCompletionSource<OrderPlaced> signal) : IConsumer<OrderPlaced>
{
    public Task ConsumeAsync(ConsumeContext<OrderPlaced> context)
    {
        signal.TrySetResult(context.Message);
        return Task.CompletedTask;
    }
}

public static class StaticMethodHandlers
{
    public static ConcurrentBag<Guid>? Log;

    // Статический метод по конвенции имени — уровень 2 API.
    public static void Handle(PlaceOrder command) => Log?.Add(command.OrderId);
}

public static class CascadeHandlers
{
    // Возврат публикуется автоматически.
    public static ReceiptRequested Handle(OrderPaid paid)
        => new(paid.OrderId, $"tx-{paid.OrderId:N}");
}

public static class TupleCascadeHandlers
{
    // Кортеж — несколько каскадных сообщений разом.
    public static (OrderPaid, ReceiptRequested) Handle(ChargeCard command)
        => (new OrderPaid(command.OrderId), new ReceiptRequested(command.OrderId, "tx-1"));
}

public static class ConditionalHandlers
{
    public static OutgoingMessages Handle(OrderPlaced placed)
    {
        var outgoing = new OutgoingMessages();

        if (placed.Total > 1_000m)
            outgoing.Send(new ChargeCard(placed.OrderId, placed.Total));

        return outgoing;
    }
}

public static class QuoteHandlers
{
    // Ответ на request: возвращаем значение, шина сама отправит его в ReplyTo.
    public static QuoteResult Handle(GetQuote request) => new(request.Symbol, 420m);
}

/// <summary>Отвечает на запрос дольше, чем таймаут — для проверки позднего ответа (идея 48).</summary>
public sealed class SlowQuoteConsumer : IConsumer<GetQuote>
{
    public async Task ConsumeAsync(ConsumeContext<GetQuote> context)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(400)).ConfigureAwait(false);
        await context.RespondAsync(new QuoteResult(context.Message.Symbol, 1m)).ConfigureAwait(false);
    }
}

public static class ReminderHandlers
{
    public static void Handle(SendReminder reminder)
    {
        // Достаточно факта обработки: его фиксирует recorder харнесса.
    }
}
