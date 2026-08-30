using AvtoBus.Abstractions;
using AvtoBus.Core;
using AvtoBus.Core.Security;
using Microsoft.Extensions.Options;

namespace AvtoBus.Persistence.Postgres;

public sealed class MessageBus : IMessageBus
{
    private readonly IMessageRegistry _registry;
    private readonly PostgresOutboxWriter _outbox;
    private readonly PostgresScheduledWriter _scheduled;
    private readonly IMessageSecurity _security;
    private readonly ISystemClock _clock;
    private readonly AvtoBusOptions _options;
    private readonly PostgresAvtoBusOptions _postgresOptions;
    private readonly MessageSecurityLimits _limits;

    public MessageBus(
        IMessageRegistry registry,
        PostgresOutboxWriter outbox,
        PostgresScheduledWriter scheduled,
        IMessageSecurity security,
        ISystemClock clock,
        IOptions<AvtoBusOptions> options,
        IOptions<PostgresAvtoBusOptions> postgresOptions,
        MessageSecurityLimits? limits = null)
    {
        _registry = registry;
        _outbox = outbox;
        _scheduled = scheduled;
        _security = security;
        _clock = clock;
        _options = options.Value;
        _postgresOptions = postgresOptions.Value;
        _limits = limits ?? new MessageSecurityLimits();
    }

    public async ValueTask EnqueueAsync<T>(
        object session,
        T message,
        PublishOptions options,
        CancellationToken cancellationToken)
        where T : IMessage
    {
        if (string.IsNullOrWhiteSpace(options.Destination))
            throw new ArgumentException("Destination is required.", nameof(options));
        if (session is not AvtoBusDbSession dbSession)
            throw new ArgumentException("Session must be AvtoBusDbSession.", nameof(session));
        var descriptor = _registry.GetByClrType(typeof(T));
        var encoded = descriptor.Encode(message, _options.Source, options, _clock);
        SecurityValidator.ValidateEnvelope(encoded.Envelope, encoded.TransportHeaders, _limits);
        if (encoded.Envelope.Length > _postgresOptions.MaxEnvelopeBytes)
            throw new PermanentMessageException("message_too_large", true);
        encoded = await _security.ProtectAsync(encoded, cancellationToken);
        SecurityValidator.ValidateEnvelope(encoded.Envelope, encoded.TransportHeaders, _limits);
        await _outbox.EnqueueAsync(
            dbSession, encoded, cancellationToken: cancellationToken);
        dbSession.OnCommitted(_outbox.NotifyCommitted);
    }

    public async ValueTask<Guid> ScheduleAsync<T>(
        object session,
        T message,
        DateTimeOffset dueAt,
        ScheduleOptions options,
        CancellationToken cancellationToken)
        where T : IMessage
    {
        if (string.IsNullOrWhiteSpace(options.Destination))
            throw new ArgumentException("Destination is required.", nameof(options));
        if (session is not AvtoBusDbSession dbSession)
            throw new ArgumentException("Session must be AvtoBusDbSession.", nameof(session));
        if (dueAt <= _clock.UtcNow)
            throw new ArgumentOutOfRangeException(nameof(dueAt), "dueAt must be in the future.");

        var descriptor = _registry.GetByClrType(typeof(T));
        var encoded = descriptor.Encode(message, _options.Source, options, _clock);
        SecurityValidator.ValidateEnvelope(encoded.Envelope, encoded.TransportHeaders, _limits);
        if (encoded.Envelope.Length > _postgresOptions.MaxEnvelopeBytes)
            throw new PermanentMessageException("message_too_large", true);
        encoded = await _security.ProtectAsync(encoded, cancellationToken);
        var scheduleId = await _scheduled.ScheduleAsync(
            dbSession, encoded, dueAt, options, cancellationToken);
        dbSession.OnCommitted(_scheduled.NotifyCommitted);
        return scheduleId;
    }
}
