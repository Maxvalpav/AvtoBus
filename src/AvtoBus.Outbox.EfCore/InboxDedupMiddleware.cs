using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using AvtoBus.Pipeline;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Inbox-дедупликация: повторное доставленное сообщение с тем же MessageId потребителю — тихо пускаем мимо.
/// Оптимистично: конфликт уникального ключа = дубликат (док 15, §6).
/// </summary>
public sealed class InboxDedupMiddleware : IBusMiddleware
{
    private readonly string _consumerId;

    public InboxDedupMiddleware(string consumerId) => _consumerId = consumerId;

    [Obsolete("Use InboxDedupMiddleware(string consumerId) — IServiceScopeFactory больше не нужен, inbox теперь в том же скоупе что хендлер.")]
    public InboxDedupMiddleware(IServiceScopeFactory _, string consumerId) : this(consumerId) { }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var consumerId = ctx.Envelope.Header("consumer") ?? _consumerId;
        var db = ctx.Services.GetService<DbContext>();

        // Без EF — дедупликация только в памяти (через InboxDeduplication), здесь пропускаем.
        if (db is null)
        {
            await next(ctx).ConfigureAwait(false);
            return;
        }

        // Быстрая проверка до выполнения хендлера — избегаем повторной работы.
        var exists = await db.Set<InboxRecord>()
            .AnyAsync(r => r.MessageId == ctx.Envelope.MessageId && r.ConsumerId == consumerId, ctx.CancellationToken)
            .ConfigureAwait(false);
        if (exists)
            return;

        // Добавляем запись, но не сохраняем отдельно — сохранится вместе с бизнес-данными
        // в той же транзакции (один SaveChanges в конце обработки).
        var timeProvider = ctx.Services.GetService<TimeProvider>() ?? TimeProvider.System;
        db.Set<InboxRecord>().Add(new InboxRecord
        {
            MessageId = ctx.Envelope.MessageId,
            ConsumerId = consumerId,
            ProcessedAt = timeProvider.GetUtcNow().UtcDateTime,
        });

        try
        {
            await next(ctx).ConfigureAwait(false);

            // Если хендлер уже вызвал SaveChanges (бизнес + inbox в одной транзакции),
            // этот SaveChanges будет no-op (нет изменений) или сохранит только inbox,
            // если хендлер не использует EF. В обоих случаях inbox фиксируется после успеха.
            await db.SaveChangesAsync(ctx.CancellationToken).ConfigureAwait(false);
        }
        catch (DbUpdateException ex) when (IsUniqueViolation(ex))
        {
            // Гонка: два консьюмера одновременно прошли AnyAsync — уникальный ключ ловит дубль.
            return;
        }
    }

    private static bool IsUniqueViolation(DbUpdateException ex)
    {
        var inner = ex.InnerException;
        if (inner is not null && inner.GetType().Name == "PostgresException")
        {
            var sqlState = inner.GetType().GetProperty("SqlState")?.GetValue(inner) as string;
            if (sqlState == "23505") return true;
        }

        return inner?.Message.Contains("UNIQUE", StringComparison.OrdinalIgnoreCase) == true
               || inner?.Message.Contains("duplicate key", StringComparison.OrdinalIgnoreCase) == true
               || inner?.Message.Contains("23505", StringComparison.OrdinalIgnoreCase) == true;
    }
}
