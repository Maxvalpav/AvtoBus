using System.Buffers;
using AvtoBus.Serialization.MessagePack;
using AvtoBus.Serialization.Protobuf;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests.Serialization;

/// <summary>
/// Бинарные сериализаторы (B11): MessagePack и Protobuf как дефолт шины.
/// Проверяем round-trip и сквозную доставку с распознаванием формата по Content-Type.
/// </summary>
public sealed class BinarySerializerTests
{
    // ---- MessagePack -----------------------------------------------------

    [Fact]
    public void MessagePack_round_trips_contract()
    {
        var serializer = new MessagePackBusSerializer();
        var expected = new OrderPlaced(Guid.NewGuid(), 199.99m);

        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(buffer, expected, typeof(OrderPlaced));

        var restored = Assert.IsType<OrderPlaced>(
            serializer.Deserialize(buffer.WrittenMemory, typeof(OrderPlaced)));

        Assert.Equal(expected, restored);
    }

    [Fact]
    public async Task MessagePack_as_default_delivers_over_the_bus()
    {
        var received = new TaskCompletionSource<LocalJob>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .UseMessagePack()
                .AddConsumer<MessagePackJobConsumer>(),
            services => services.AddSingleton(received));

        var job = new LocalJob(Guid.NewGuid(), "msgpack");
        await harness.Bus.SendAsync(job);

        Assert.Equal(job, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void MessagePack_content_type_is_recognized_by_registry()
    {
        var registry = new AvtoBus.Serialization.SerializerRegistry(new MessagePackBusSerializer());
        Assert.IsType<MessagePackBusSerializer>(registry.For("application/x-msgpack"));
    }

    // ---- Protobuf --------------------------------------------------------

    [Fact]
    public void Protobuf_round_trips_generated_message()
    {
        var serializer = new ProtobufBusSerializer();
        var expected = new Timestamp { Seconds = 1_700_000_000, Nanos = 123_456_789 };

        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(buffer, expected, typeof(Timestamp));

        var restored = Assert.IsType<Timestamp>(
            serializer.Deserialize(buffer.WrittenMemory, typeof(Timestamp)));

        Assert.Equal(expected, restored);
    }

    [Fact]
    public void Protobuf_rejects_non_protobuf_contract()
    {
        var serializer = new ProtobufBusSerializer();
        var buffer = new ArrayBufferWriter<byte>();

        Assert.Throws<NotSupportedException>(() => serializer.Serialize(buffer, new LocalJob(Guid.NewGuid(), "x"), typeof(LocalJob)));
        Assert.Throws<NotSupportedException>(() => serializer.Deserialize(buffer.WrittenMemory, typeof(LocalJob)));
    }

    [Fact]
    public async Task Protobuf_as_default_delivers_over_the_bus()
    {
        var received = new TaskCompletionSource<Timestamp>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .UseProtobuf()
                .AddConsumer<ProtobufTimestampConsumer>(),
            services => services.AddSingleton(received));

        var stamp = new Timestamp { Seconds = 1_700_000_000 };
        await harness.Bus.SendAsync(stamp);

        Assert.Equal(stamp, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }
}

public sealed class MessagePackJobConsumer(TaskCompletionSource<LocalJob> signal) : IConsumer<LocalJob>
{
    public Task ConsumeAsync(ConsumeContext<LocalJob> context)
    {
        signal.TrySetResult(context.Message);
        return Task.CompletedTask;
    }
}

public sealed class ProtobufTimestampConsumer(TaskCompletionSource<Timestamp> signal) : IConsumer<Timestamp>
{
    public Task ConsumeAsync(ConsumeContext<Timestamp> context)
    {
        signal.TrySetResult(context.Message);
        return Task.CompletedTask;
    }
}
