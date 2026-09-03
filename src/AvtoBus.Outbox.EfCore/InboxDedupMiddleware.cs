using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using AvtoBus.Pipeline;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Inbox-дедупликация: повторное доставленное сообщение с тем же MessageId потребителю — тихо пускаем мимо.
/// Оптимистично: конфликт уникального ключа = дубликат (док 15, §6).
/// </summary>
public class InboxDedupMiddleware : IBusMiddleware
{
    private readonly string _consumerId;

    public InboxDedupMiddleware(string consumerId) => _consumerId = consumerId;

    /// <summary>
    /// Окно дедупликации: записи старше игнорируются (дефолт 7 дней = <c>OutboxOptions.CleanupAfter</c>).
    /// In-memory путь настраивается через <c>BusOptions.InboxWindow</c>; здесь — свойством,
    /// т.к. middleware создаётся вручную с consumerId.
    /// </summary>
    public TimeSpan InboxWindow { get; set; } = TimeSpan.FromDays(7);

    [Obsolete("Use InboxDedupMiddleware(string consumerId) — IServiceScopeFactory больше не нужен, inbox теперь в том же скоупе что хендлер.", DiagnosticId = "AVB0001", UrlFormat = "https://github.com/Maxvalpav/AvtoBus/blob/main/docs/15-implementation-outbox.md#inbox")]
    public InboxDedupMiddleware(IServiceScopeFactory _, string consumerId) : this(consumerId) { }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        // Аудит A4: заголовок "consumer" из конверта НЕ доверенный (не входит в подпись) —
        // им управляет отправитель. Ключ дедупликации берём только из конфигурации подписки.
        var consumerId = _consumerId;
        var db = ResolveDbContext(ctx);

        if (db is null)
        {
            await next(ctx).ConfigureAwait(false);
            return;
        }

        // Pre-check до вызова хендлера: без него дубликат выполнял хендлер,
        // а уникальный ключ срабатывал лишь на SaveChanges — уже после обработки.
        // Фильтр по окну: старые записи чистка уже удалила, повтор вне окна — не дубликат.
        var messageId = ctx.Envelope.MessageId;
        var cutoff = (ctx.Services.GetService<TimeProvider>() ?? TimeProvider.System).GetUtcNow().UtcDateTime - InboxWindow;
        var seen = await db.Set<InboxRecord>().AnyAsync(
            r => r.MessageId == messageId && r.ConsumerId == consumerId && r.ProcessedAt >= cutoff,
            ctx.CancellationToken).ConfigureAwait(false);
        if (seen)
            return;

        // Добавляем запись, но не сохраняем отдельно — сохранится вместе с бизнес-данными
        // в той же транзакции (один SaveChanges в конце обработки).
        var timeProvider = ctx.Services.GetService<TimeProvider>() ?? TimeProvider.System;
        db.Set<InboxRecord>().Add(new InboxRecord
        {
            MessageId = messageId,
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
            // НО: глушим только конфликт inbox-таблицы. Бизнес-конфликт уникальности
            // (свой INSERT хендлера) раньше маскировался под дубликат — сообщение молча
            // подтверждалось без ретрая/DLQ (потеря данных). Чужой конфликт — проброс.
            if (IsInboxConstraint(ex))
                return;
            throw;
        }
    }

    /// <summary>
    /// Выбор DbContext для inbox-записи. По умолчанию — первый зарегистрированный
    /// (недетерминирован при нескольких контекстах, см. аудит A4): при нескольких
    /// контекстах используйте <see cref="InboxDedupMiddleware{TDbContext}"/> с явным типом.
    /// </summary>
    protected virtual DbContext? ResolveDbContext(ConsumeContext ctx)
        => ctx.Services.GetServices<DbContext>().FirstOrDefault()
           ?? ctx.Services.GetService(typeof(DbContext)) as DbContext;

    private static bool IsUniqueViolation(DbUpdateException ex)
    {
        for (var e = ex.InnerException; e is not null; e = e.InnerException)
        {
            // PostgreSQL: SqlState 23505 — код состояния не зависит от lc_messages (аудит A5).
            var sqlState = e.GetType().GetProperty("SqlState")?.GetValue(e) as string;
            if (sqlState == "23505") return true;

            // SQL Server: error numbers 2627 (unique constraint) / 2601 (unique index).
            if (e.GetType().GetProperty("Number")?.GetValue(e) is int number
                && (number is 2627 or 2601)) return true;

            // SQLite: SqliteErrorCode / Result 19 (SQLITE_CONSTRAINT).
            if (e.GetType().GetProperty("SqliteErrorCode")?.GetValue(e) is int sqliteCode && sqliteCode == 19)
                return true;
            if (e.GetType().GetProperty("SqliteExtendedErrorCode")?.GetValue(e) is int extCode && extCode / 256 == 19)
                return true;

            // MySQL: error numbers 1062 (duplicate entry).
            if (e.GetType().GetProperty("Number")?.GetValue(e) is int mysqlNumber && mysqlNumber == 1062
                && e.GetType().FullName?.Contains("MySql", StringComparison.OrdinalIgnoreCase) == true) return true;
        }

        // Текстовый fallback — последний шанс для неизвестных провайдеров (локале-зависим, см. аудит A5).
        var inner = ex.InnerException;
        return inner?.Message.Contains("UNIQUE", StringComparison.OrdinalIgnoreCase) == true
               || inner?.Message.Contains("duplicate key", StringComparison.OrdinalIgnoreCase) == true
               || inner?.Message.Contains("23505", StringComparison.OrdinalIgnoreCase) == true;
    }

    /// <summary>
    /// Конфликт именно inbox-таблицы (а не бизнес-уникальности хендлера):
    /// имя ограничения PK_avtobus_inbox / таблица avtobus_inbox в сообщении или
    /// ConstraintName провайдера (Npgsql: PostgresException.ConstraintName).
    /// </summary>
    private static bool IsInboxConstraint(DbUpdateException ex)
    {
        for (var e = ex.InnerException; e is not null; e = e.InnerException)
        {
            var constraint = e.GetType().GetProperty("ConstraintName")?.GetValue(e) as string;
            if (constraint is not null)
                return constraint.Contains("avtobus_inbox", StringComparison.OrdinalIgnoreCase);
            var msg = e.Message;
            if (msg.Contains("avtobus_inbox", StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}

/// <summary>
/// Типизированный inbox-дедуп с явным DbContext (аудит A4): при нескольких контекстах
/// в приложении гарантирует запись inbox в ту же БД, что и бизнес-данные.
/// </summary>
/// <typeparam name="TDbContext">Контекст, в котором лежат бизнес-данные и inbox-таблица.</typeparam>
public sealed class InboxDedupMiddleware<TDbContext>(string consumerId) : InboxDedupMiddleware(consumerId)
    where TDbContext : DbContext
{
    protected override DbContext? ResolveDbContext(ConsumeContext ctx)
        => ctx.Services.GetService<TDbContext>() as DbContext
           ?? base.ResolveDbContext(ctx);
}
