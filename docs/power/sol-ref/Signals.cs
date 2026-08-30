using System.Threading.Channels;

namespace AvtoBus.Persistence.Postgres;

public interface IOutboxSignal
{
    void Pulse();
    ValueTask WaitAsync(CancellationToken cancellationToken);
}

public sealed class OutboxSignal : IOutboxSignal
{
    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = false,
            SingleWriter = false
        });

    public void Pulse() => _channel.Writer.TryWrite(true);

    public async ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        await _channel.Reader.ReadAsync(cancellationToken);
        while (_channel.Reader.TryRead(out _)) { }
    }
}

public interface IScheduledSignal
{
    void Pulse();
    ValueTask WaitAsync(CancellationToken cancellationToken);
}

public sealed class ScheduledSignal : IScheduledSignal
{
    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = false,
            SingleWriter = false
        });

    public void Pulse() => _channel.Writer.TryWrite(true);

    public async ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        await _channel.Reader.ReadAsync(cancellationToken);
        while (_channel.Reader.TryRead(out _)) { }
    }
}
