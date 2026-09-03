using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Outbox.EfCore;

/// <summary>Запись transactional outbox (док 15, §1).</summary>
public sealed class OutboxMessage
{
    public long Id { get; set; }
    public Guid MessageId { get; set; }
    public string Destination { get; set; } = "";
    public string Transport { get; set; } = "";
    /// <summary>Вид назначения: 0 — очередь, 1 — топик (см. <c>DestinationKind</c>).</summary>
    public int Kind { get; set; }
    public string MessageType { get; set; } = "";
    public string? PartitionKey { get; set; }
    public string? TenantId { get; set; }
    public byte[] EnvelopeBlob { get; set; } = [];
    public DateTime CreatedAt { get; set; }
    public DateTime? SendAfter { get; set; }
    public DateTime? SentAt { get; set; }
    public DateTime? ClaimedAt { get; set; }
    public string? ClaimedBy { get; set; }
    public int Attempt { get; set; }
    public string? LastError { get; set; }
}

/// <summary>Запись inbox-дедупликации (док 15, §1).</summary>
public sealed class InboxRecord
{
    public Guid MessageId { get; set; }

    public string ConsumerId { get; set; } = "";

    public DateTime ProcessedAt { get; set; }

    public byte[]? Response { get; set; }
}

/// <summary>
/// Лиза партиции outbox (аудит A1): какой relay-инстанс прямо сейчас владеет ключом
/// и до когда. Даёт FIFO per PartitionKey при нескольких relay без PG-специфичных
/// advisory-локов — обычный PK + условные UPDATE/INSERT, работает на любом провайдере.
/// Просроченная лиза перехватывается (relay умер между acquire и release).
/// </summary>
public sealed class OutboxPartitionLease
{
    public string PartitionKey { get; set; } = "";

    public string Owner { get; set; } = "";

    public DateTime ExpiresAt { get; set; }
}

/// <summary>Конфигурация EF-модели outbox/inbox (док 15, §1).</summary>
public static class OutboxModelBuilder
{
    public static ModelBuilder ConfigureOutbox(this ModelBuilder mb)
    {
        mb.Entity<OutboxMessage>(e =>
        {
            e.ToTable("avtobus_outbox");
            e.HasKey(x => x.Id);
            e.HasIndex(x => x.MessageId).IsUnique();
            e.HasIndex(x => new { x.SentAt, x.SendAfter, x.ClaimedAt, x.Id })
                .HasFilter("\"SentAt\" IS NULL");
            e.Property(x => x.EnvelopeBlob).HasColumnType("bytea");
        });

        mb.Entity<InboxRecord>(e =>
        {
            e.ToTable("avtobus_inbox");
            e.HasKey(x => new { x.MessageId, x.ConsumerId });
            e.HasIndex(x => x.ProcessedAt);
        });

        mb.Entity<OutboxPartitionLease>(e =>
        {
            e.ToTable("avtobus_outbox_leases");
            e.HasKey(x => x.PartitionKey);
        });

        return mb;
    }
}
