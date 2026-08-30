namespace AvtoBus.Abstractions;

[Flags]
public enum AvtoTransportCapabilities
{
    None = 0,
    Queues = 1,
    Topics = 2,
    ConsumerGroups = 4,
    PartitionOrdering = 8,
    DelayedDelivery = 16,
    NativeDeadLetter = 32,
    Replay = 64,
    Transactions = 128,
    OffsetCommit = 256,
    Compaction = 512,
    Sessions = 1024,
    PullConsumers = 2048,
    CloudEventsNative = 4096,
}

public sealed record AvtoOutgoing(AvtoEnvelope Envelope, string Destination);

public delegate Task<bool> AvtoDeliveryHandler(AvtoEnvelope envelope, CancellationToken ct);

public interface IAvtoTransport : IAsyncDisposable
{
    string Name { get; }
    AvtoTransportCapabilities Capabilities { get; }

    ValueTask SendAsync(AvtoOutgoing outgoing, CancellationToken ct);

    ValueTask<IAsyncDisposable> SubscribeAsync(
        string endpoint,
        AvtoDeliveryHandler handler,
        CancellationToken ct);
}
