using AvtoBus;
using AvtoBus.Configuration;
using AvtoBus.InMemory;
using AvtoBus.Outbox.EfCore;
using AvtoBus.Runtime;
using AvtoBus.Scheduling;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AvtoBus.Tests.Scheduling;

public class SchedulerServiceTests
{
    private static (SchedulerService Service, Runtime.EnvelopeFactory Factory, JsonEnvelopeSerializer Codec) Build(
        InMemoryScheduleStore store,
        InMemoryTransport transport,
        TimeProvider clock,
        ILeaderElection? leader = null,
        TimeSpan? poll = null)
    {
        var transports = new TransportRegistry([transport], "inmemory");
        var options = new BusOptions();
        var registry = MessageRegistry.Build([typeof(Ping)]);
        var factory = new Runtime.EnvelopeFactory(options, registry, clock);
        var codec = new JsonEnvelopeSerializer();

        var scheduler = new SchedulerService(
            store,
            transports,
            new EnvelopeCodecFactory(factory, codec),
            leader ?? new InMemoryLeaderElection(),
            new SchedulerOptions
            {
                PollInterval = poll ?? TimeSpan.FromMilliseconds(10),
                CronPollInterval = poll ?? TimeSpan.FromMilliseconds(10),
            },
            clock,
            NullLogger<SchedulerService>.Instance);

        return (scheduler, factory, codec);
    }

    [Fact]
    public async Task Delivers_due_scheduled_message_to_queue()
    {
        var store = new InMemoryScheduleStore();
        var transport = new InMemoryTransport();

        var (scheduler, factory, codec) = Build(store, transport, TimeProvider.System);

        var envelope = factory.Create(new Ping(DateTime.UtcNow.Ticks), typeof(Ping), null, null);
        await store.ScheduleAsync(new ScheduledMessage
        {
            Token = Guid.NewGuid(),
            MessageType = envelope.MessageType,
            EnvelopeBlob = codec.Serialize(envelope),
            Destination = "ping-queue",
            DeliverAt = DateTime.UtcNow.AddMilliseconds(50),
        });

        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            var delivered = WaitFor(() =>
            {
                transport.PumpDelayedAsync().GetAwaiter().GetResult();
                return transport.QueueDepths.GetValueOrDefault("ping-queue") > 0;
            }, TimeSpan.FromSeconds(5));

            Assert.True(delivered, "Scheduled message wasn't delivered to the queue within timeout");
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Cron_fires_on_topic_when_due()
    {
        var store = new InMemoryScheduleStore();
        var transport = new InMemoryTransport();
        var leader = new InMemoryLeaderElection();

        var (scheduler, factory, codec) = Build(store, transport, TimeProvider.System, leader);

        var envelope = factory.Create(new Ping(1), typeof(Ping), null, null);
        var topic = envelope.MessageType; // "scheduling.ping"

        await store.UpsertCronAsync(new CronSchedule
        {
            Name = "sec",
            CronExpression = "*/1 * * * * *",
            MessageType = topic,
            PayloadBlob = codec.Serialize(envelope),
            NextFireAt = DateTime.UtcNow.AddMilliseconds(50),
        });

        // Подписываем группу заранее, чтобы поймать первое срабатывание.
        transport.BindSubscription(topic, "test");

        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            var received = WaitFor(() =>
            {
                transport.PumpDelayedAsync().GetAwaiter().GetResult();
                return transport.QueueDepths.GetValueOrDefault($"{topic}::test") > 0;
            }, TimeSpan.FromSeconds(5));

            Assert.True(received, "Cron didn't fire into the topic within timeout");
            Assert.True(leader.IsLeader("avtobus-cron"));
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    private static bool WaitFor(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                return false;
            Thread.Sleep(50);
        }
        return true;
    }

    public sealed record Ping(long At);
}
