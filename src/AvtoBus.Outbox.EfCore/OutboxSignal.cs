using System.Threading.Channels;

namespace AvtoBus.Outbox.EfCore;

/// <summary>Сигнал процессного relay: «в БД появились новые outbox-сообщения».</summary>
public interface IOutboxSignal
{
    void Nudge();

    Task WaitAsync(TimeSpan timeout, CancellationToken ct);
}

/// <summary>In-process сигнал на Channel: один буфер, перезапись при переполнении — ждать незачем.</summary>
public sealed class ChannelOutboxSignal : IOutboxSignal
{
    private readonly Channel<byte> _ch =
        Channel.CreateBounded<byte>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public void Nudge() => _ch.Writer.TryWrite(0);

    public async Task WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try { await _ch.Reader.ReadAsync(cts.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) { }
    }
}
