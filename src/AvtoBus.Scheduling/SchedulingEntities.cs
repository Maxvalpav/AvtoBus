using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Scheduling;

/// <summary>EF-сущности отложенных сообщений и cron-расписаний (идеи 223, 226).</summary>
public static class SchedulingEntities
{
    public static ModelBuilder ConfigureScheduling(this ModelBuilder mb)
    {
        mb.Entity<ScheduledMessage>(e =>
        {
            e.ToTable("avtobus_scheduled");
            e.HasKey(x => x.Id);
            e.Property(x => x.Token).ValueGeneratedNever();
            e.HasIndex(x => x.Token).IsUnique();
            e.HasIndex(x => new { x.DeliverAt, x.DeliveredAt, x.CancelledAt })
                .HasFilter("\"DeliveredAt\" IS NULL AND \"CancelledAt\" IS NULL");
            e.Property(x => x.EnvelopeBlob).HasColumnType("bytea");
        });

        mb.Entity<CronSchedule>(e =>
        {
            e.ToTable("avtobus_cron");
            e.HasKey(x => x.Id);
            e.Property(x => x.Name);
            e.HasIndex(x => x.Name).IsUnique();
            e.HasIndex(x => x.NextFireAt);
            e.Property(x => x.PayloadBlob).HasColumnType("bytea");
        });

        return mb;
    }
}
