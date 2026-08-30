using AvtoBus;
using AvtoBus.Outbox.EfCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Scheduling;

/// <summary>
/// Синхронизирует cron-регистрации из кода (ICronRegistry) в durable-хранилище при старте (идея 223).
/// </summary>
internal sealed class CronBootstrapper(
    ICronRegistry registry,
    IScheduleStore store,
    IEnvelopeFactory envelopeFactory,
    TimeProvider clock,
    ILogger<CronBootstrapper> log) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        foreach (var reg in registry.Registrations)
        {
            var cron = CronExpression.Parse(reg.CronExpression);
            var tz = TimeZoneInfo.FindSystemTimeZoneById(reg.TimeZoneId);
            var next = cron.GetNextOccurrence(clock.GetUtcNow(), tz) ?? clock.GetUtcNow().AddYears(1);

            var envelope = envelopeFactory.Create(reg.Payload, reg.PayloadType, options: null, parent: null);

            await store.UpsertCronAsync(new CronSchedule
            {
                Name = reg.Name,
                CronExpression = reg.CronExpression,
                TimeZoneId = reg.TimeZoneId,
                MessageType = envelope.MessageType,
                PayloadBlob = envelopeFactory.Serialize(envelope),
                NextFireAt = next.UtcDateTime,
                Misfire = reg.Misfire,
                IsEnabled = true,
            }, ct);

            log.LogInformation("Cron '{Name}' registered: {Expr} ({Tz}), next {Next}",
                reg.Name, reg.CronExpression, reg.TimeZoneId, next);
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}

/// <summary>Сериализует и десериализует конверты/подписи cron, пряча JsonEnvelopeSerializer.</summary>
public interface IEnvelopeFactory
{
    Envelope Create(object message, Type messageType, MessageOptions? options, Envelope? parent);
    byte[] Serialize(Envelope envelope);
    Envelope Deserialize(ReadOnlyMemory<byte> blob);
}

public sealed class EnvelopeCodecFactory : IEnvelopeFactory
{
    private readonly Runtime.EnvelopeFactory _inner;
    private readonly IEnvelopeSerializer _bytes;

    public EnvelopeCodecFactory(Runtime.EnvelopeFactory inner, IEnvelopeSerializer bytes)
    {
        _inner = inner;
        _bytes = bytes;
    }

    public Envelope Create(object message, Type messageType, MessageOptions? options, Envelope? parent)
        => _inner.Create(message, messageType, options, parent);

    public byte[] Serialize(Envelope envelope) => _bytes.Serialize(envelope);

    public Envelope Deserialize(ReadOnlyMemory<byte> blob) => _bytes.Deserialize(blob);
}
