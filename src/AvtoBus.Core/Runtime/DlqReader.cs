using System.Runtime.CompilerServices;
using AvtoBus.Observability;

namespace AvtoBus.Runtime;

/// <summary>Сообщение из DLQ-очереди с контекстом отказа (идея 164, 165).</summary>
public sealed record DlqMessage(
    Envelope Envelope,
    string? Reason,
    string? FailedAt,
    string? OriginalDestination);

/// <summary>
/// Read-only доступ к DLQ (идеи 91, 164, 168): просмотр, реплей одного сообщения,
/// rate-limited реплей всей очереди. Работает поверх любого <see cref="ITransport"/>
/// без изменения контракта транспорта.
/// </summary>
/// <remarks>
/// Реализация на «прочитай и верни»: каждое сообщение вычитывается свежим подписчиком,
/// возвращается на место (или реплеится), и проход завершается по одногр завершённому кругу —
/// так читатель не зацикливается сам на себе и не дублирует результаты.
/// </remarks>
public sealed class DlqReader
{
    private readonly TransportRegistry _transports;
    private readonly TimeProvider _time;

    public DlqReader(TransportRegistry transports, TimeProvider? time = null)
    {
        _transports = transports;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>Группа читателя уникальна на процесс: DLQ просматривается, не конкурируя с консьюмерами.</summary>
    private static readonly string ReaderGroup = $"avtobus-dlq/{Environment.MachineName}/{Environment.ProcessId}";

    /// <summary>Сколько ждать сообщения в DLQ, прежде чем решить, что очередь пуста.</summary>
    private static readonly TimeSpan ReadTimeout = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Просматривает сообщения DLQ, не удаляя их. Каждое вычитанное сообщение возвращается
    /// в ту же очередь той же попыткой доставки. Проход заканчивается, когда очередь обошла
    /// круг (встретилось уже возвращённое сообщение) или превышен <paramref name="max"/>.
    /// </summary>
    public async Task<IReadOnlyList<DlqMessage>> BrowseAsync(
        TransportDestination dlq,
        int max = 50,
        CancellationToken ct = default)
    {
        var seen = new HashSet<Guid>();
        var messages = new List<DlqMessage>();

        while (messages.Count < max && !ct.IsCancellationRequested)
        {
            var message = await ReadOneAsync(dlq, ct).ConfigureAwait(false);
            if (message is null)
                break;

            // Возвращаем как есть: просмотр не меняет попытку доставки.
            await SendBackAsync(dlq, message, ct).ConfigureAwait(false);

            // Дежавю — очередь обошла полный круг, новых сообщений нет.
            if (!seen.Add(message.Envelope.MessageId))
                break;

            messages.Add(ToDlqMessage(message.Envelope));
        }

        return messages;
    }

    /// <summary>
    /// Переносит одно сообщение из DLQ в исходную очередь (идея 91). Исходная очередь
    /// берётся из заголовка либо задаётся явно. Сообщение удаляется из DLQ.
    /// </summary>
    public async Task<bool> ReplayAsync(
        TransportDestination dlq,
        Guid messageId,
        TransportDestination? to = null,
        CancellationToken ct = default)
    {
        var seen = new HashSet<Guid>();

        while (!ct.IsCancellationRequested)
        {
            var message = await ReadOneAsync(dlq, ct).ConfigureAwait(false);
            if (message is null)
                return false;

            if (message.Envelope.MessageId == messageId)
            {
                var target = to ?? ParseDestination(message.Envelope.Header(BusHeaders.OriginalDestination) ?? dlq.Name);
                await _transports.Default.SendAsync(message.Envelope, target, ct).ConfigureAwait(false);
                await message.AcknowledgeAsync(ct).ConfigureAwait(false);
                AvtoBusEventSource.Log.MessageReplayed(message.Envelope.MessageType, messageId.ToString("N"), target.Name);
                return true;
            }

            // Дежавю — очередь обошла полный круг, целевого сообщения в ней нет.
            if (!seen.Add(message.Envelope.MessageId))
            {
                await SendBackAsync(dlq, message, ct).ConfigureAwait(false);
                return false;
            }

            // Остальные сообщения остаются в DLQ.
            await SendBackAsync(dlq, message, ct).ConfigureAwait(false);
        }

        return false;
    }

    /// <summary>Реплей всех сообщений DLQ в исходные очереди с ограничением скорости (идея 168).
    /// Сообщения без заголовка исходной очереди остаются на месте.
    /// </summary>
    public async Task<int> ReplayAllAsync(
        TransportDestination dlq,
        int maxPerSecond = 10,
        CancellationToken ct = default)
    {
        var replayed = 0;
        var seen = new HashSet<Guid>();
        var minInterval = maxPerSecond > 0 ? TimeSpan.FromSeconds(1) / maxPerSecond : TimeSpan.Zero;
        var nextSlot = _time.GetUtcNow();

        while (!ct.IsCancellationRequested)
        {
            var message = await ReadOneAsync(dlq, ct).ConfigureAwait(false);
            if (message is null)
                break;

            var original = ParseDestinationOrNull(message.Envelope.Header(BusHeaders.OriginalDestination));
            if (original is null)
            {
                if (!seen.Add(message.Envelope.MessageId))
                {
                    // Дежавю: все такие сообщения обойдены — завершаем, возвращая его на место.
                    await SendBackAsync(dlq, message, ct).ConfigureAwait(false);
                    break;
                }

                await SendBackAsync(dlq, message, ct).ConfigureAwait(false);
                continue;
            }

            // Rate limit: не более maxPerSecond сообщений в секунду (seed-flood защита, идея 168).
            if (minInterval > TimeSpan.Zero)
            {
                var now = _time.GetUtcNow();
                if (now < nextSlot)
                {
                    try { await Task.Delay(nextSlot - now, _time, ct).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return replayed; }
                }

                nextSlot = _time.GetUtcNow() + minInterval;
            }

            await _transports.Default.SendAsync(message.Envelope, original.Value, ct).ConfigureAwait(false);
            await message.AcknowledgeAsync(ct).ConfigureAwait(false);
            replayed++;
        }

        return replayed;
    }

    /// <summary>
    /// Удаляет одно сообщение из DLQ (например, по явному распоряжению оператора через дашборд).
    /// Возвращает true, если сообщение найдено и удалено.
    /// </summary>
    public async Task<bool> DeleteAsync(
        TransportDestination dlq,
        Guid messageId,
        CancellationToken ct = default)
    {
        var seen = new HashSet<Guid>();

        while (!ct.IsCancellationRequested)
        {
            var message = await ReadOneAsync(dlq, ct).ConfigureAwait(false);
            if (message is null)
                return false;

            if (message.Envelope.MessageId == messageId)
            {
                await message.AcknowledgeAsync(ct).ConfigureAwait(false);
                return true;
            }

            // Дежавю — очередь обошла полный круг, целевого сообщения в ней нет.
            if (!seen.Add(message.Envelope.MessageId))
            {
                await SendBackAsync(dlq, message, ct).ConfigureAwait(false);
                return false;
            }

            await SendBackAsync(dlq, message, ct).ConfigureAwait(false);
        }

        return false;
    }

    private static DlqMessage ToDlqMessage(Envelope envelope) => new(
        envelope,
        envelope.Header(BusHeaders.DeadLetterReason),
        envelope.Header(BusHeaders.FailedAt),
        envelope.Header(BusHeaders.OriginalDestination));

    /// <summary>
    /// Вычитывает одно сообщение из DLQ и закрывает подписку. Каждый вызов стартует
    /// свежим подписчиком с начала очереди — возвращённое сообщение не перечитывается.
    /// </summary>
    private async Task<ITransportMessage?> ReadOneAsync(TransportDestination dlq, CancellationToken ct)
    {
        var subscription = new TransportSubscription(dlq, ReaderGroup, PrefetchCount: 1);

        using var lease = CancellationTokenSource.CreateLinkedTokenSource(ct);
        lease.CancelAfter(ReadTimeout);

        try
        {
            await using var enumerator = _transports.Default
                .ReceiveAsync(subscription, lease.Token)
                .GetAsyncEnumerator(lease.Token);

            if (!await enumerator.MoveNextAsync())
                return null;

            return enumerator.Current;
        }
        catch (OperationCanceledException)
        {
            // Таймаут: очередь пуста.
            return null;
        }
    }

    /// <summary>
    /// Возвращает сообщение в ту же очередь — просмотр/пропуск не теряют данные.
    /// Вычитывание удаляет сообщение из канала транспорта, поэтому пишем обратно копию.
    /// </summary>
    private async ValueTask SendBackAsync(
        TransportDestination dlq,
        ITransportMessage message,
        CancellationToken ct)
    {
        await _transports.Default.SendAsync(message.Envelope, dlq, ct).ConfigureAwait(false);
        await message.AcknowledgeAsync(ct).ConfigureAwait(false);
    }

    /// <summary>"queue:name" / "topic:name" → назначение. Неизвестный формат — очередь.</summary>
    internal static TransportDestination ParseDestination(string value)
        => ParseDestinationOrNull(value) ?? TransportDestination.Queue(value);

    private static TransportDestination? ParseDestinationOrNull(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var colon = value.IndexOf(':');
        if (colon <= 0)
            return TransportDestination.Queue(value);

        var kind = value[..colon];
        var name = value[(colon + 1)..];

        return kind.Equals("topic", StringComparison.OrdinalIgnoreCase)
            ? TransportDestination.Topic(name)
            : TransportDestination.Queue(name);
    }
}
