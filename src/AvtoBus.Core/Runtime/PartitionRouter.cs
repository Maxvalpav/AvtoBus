using System.Threading.Channels;
using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


namespace AvtoBus.Runtime;

/// <summary>
/// Раскладывает сообщения по партициям-каналам: один ключ — всегда один канал,
/// поэтому порядок в рамках ключа сохраняется при параллельной обработке разных ключей (идея 25).
/// </summary>
internal sealed class PartitionRouter : IAsyncDisposable
{
    private readonly Channel<(ITransportMessage Message, Func<ITransportMessage, CancellationToken, Task> Handler)>[] _partitions;
    private readonly Task[] _workers;
    private readonly Func<object, string>? _keySelector;

    private readonly CancellationTokenSource _shutdown = new();
    public PartitionRouter(int partitions, Func<object, string>? keySelector, int boundedCapacity = 100, ILogger? logger = null)
    {
        _keySelector = keySelector;
        _partitions = new Channel<(ITransportMessage, Func<ITransportMessage, CancellationToken, Task>)>[partitions];
        _workers = new Task[partitions];

        for (var i = 0; i < partitions; i++)
        {
            var channel = Channel.CreateBounded<(ITransportMessage, Func<ITransportMessage, CancellationToken, Task>)>(
                new BoundedChannelOptions(boundedCapacity) { SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait });

            _partitions[i] = channel;
            var captured = channel;
            _workers[i] = Task.Run(async () =>
            {
                await foreach (var (message, handler) in captured.Reader.ReadAllAsync(_shutdown.Token).ConfigureAwait(false))
                {
                    try { await handler(message, _shutdown.Token).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (_shutdown.IsCancellationRequested) { break; }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Partition worker упал, но продолжает работу");
                    }
                }
            }, CancellationToken.None);
        }
    }

    public ValueTask EnqueueAsync(
        ITransportMessage message,
        Func<ITransportMessage, CancellationToken, Task> handler,
        CancellationToken ct)
    {
        var index = PartitionOf(message);
        return _partitions[index].Writer.WriteAsync((message, handler), ct);
    }

    /// <summary>
    /// Ключ берём из конверта: он проставлен на отправке и доступен без десериализации тела.
    /// Нет ключа — распределяем по MessageId, порядок в этом случае не гарантируется.
    /// </summary>
    private int PartitionOf(ITransportMessage message)
    {
        var key = message.Envelope.PartitionKey ?? message.Envelope.MessageId.ToString();
        var hash = StableHash(key);
        return (int)(hash % (uint)_partitions.Length);
    }

    private static uint StableHash(string s)
    {
        // FNV-1a 32-bit stable across processes
        uint h = 2166136261u;
        foreach (var ch in s)
        {
            h ^= ch;
            h *= 16777619u;
        }
        return h;
    }

    public void Complete()
    {
        foreach (var partition in _partitions)
            partition.Writer.TryComplete();
    }

    /// <summary>Дожидается завершения всех партиций: канал закрыт, воркеры дочитали остаток.</summary>
    public async Task DrainAsync(CancellationToken ct)
    {
        foreach (var partition in _partitions)
            await partition.Reader.Completion.WaitAsync(ct).ConfigureAwait(false);

        await Task.WhenAll(_workers).WaitAsync(ct).ConfigureAwait(false);
    }

    private int _disposed;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;
        Complete();
        try { await Task.WhenAll(_workers).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); } catch { }
        try { await _shutdown.CancelAsync().ConfigureAwait(false); } catch (ObjectDisposedException) { }
        _shutdown.Dispose();
    }
}
