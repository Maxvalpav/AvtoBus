using AvtoBus.Core;

namespace AvtoBus.Persistence.Postgres;

public sealed class DuplicateOutboxEventException(Guid eventId)
    : Exception($"Outbox event '{eventId}' already exists.");

public sealed class TransportPublishException(string message) : Exception(message);

public sealed class ProcessConcurrencyException(string processType, Guid correlationId)
    : Exception($"Process '{processType}/{correlationId}' was changed concurrently.");
