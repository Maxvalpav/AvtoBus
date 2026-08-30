using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using AvtoBus.Observability;
using AvtoBus.Pipeline;

namespace AvtoBus.Runtime;

/// <summary>
/// Событийный «чёрный список» на лету (идея 349): оператор может заблокировать тип или
/// паттерн сообщений, не останавливая сервис — например, забагованный продюсер спамит кассу.
/// Заблокированные консьюмеры дропают до фикса; всё попадает в метрику и журнал.
/// </summary>
public sealed class BlacklistRegistry
{
    private readonly ConcurrentDictionary<string, byte> _patterns = new(StringComparer.Ordinal);
    private readonly ILogger<BlacklistRegistry> _logger;

    public BlacklistRegistry(ILogger<BlacklistRegistry>? logger = null)
        => _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<BlacklistRegistry>.Instance;

    /// <summary>Заблокированные паттерны: точное имя типа (CLR) или wildcard вида <c>orders.*</c>.</summary>
    public IReadOnlyCollection<string> Patterns => _patterns.Keys.ToArray();

    public bool IsBlocked(string messageType)
    {
        if (_patterns.ContainsKey(messageType))
            return true;

        // Wildcard-паттерн «prefix.*» закрывает тип целиком (идея 349).
        var dotIndex = messageType.LastIndexOf('.');
        while (dotIndex > 0)
        {
            var prefix = messageType[..(dotIndex + 1)];
            if (_patterns.ContainsKey(prefix + '*'))
                return true;

            dotIndex = messageType.LastIndexOf('.', dotIndex - 1);
        }

        return false;
    }

    public IReadOnlyCollection<string> Block(string pattern)
    {
        _patterns.TryAdd(pattern, 0);
        _logger.LogWarning("В чёрный список добавлен паттерн {Pattern}", pattern);
        return Patterns;
    }

    public IReadOnlyCollection<string> Unblock(string pattern)
    {
        _patterns.TryRemove(pattern, out _);
        _logger.LogInformation("Паттерн {Pattern} снят с чёрного списка", pattern);
        return Patterns;
    }
}

/// <summary>
/// Дропает сообщения, попавшие под <see cref="BlacklistRegistry"/>, до любых хендлеров.
/// Должен стоять в начале цепочки (идея 349).
/// </summary>
public sealed class BlacklistMiddleware(BlacklistRegistry registry, ILogger<BlacklistMiddleware>? logger = null) : IBusMiddleware
{
    private readonly ILogger<BlacklistMiddleware> _logger =
        logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<BlacklistMiddleware>.Instance;

    public ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var messageType = context.Message.GetType().Name;

        if (registry.IsBlocked(messageType))
        {
            // Против правил безопаснее, чем обрабатывать заведомо битое сообщение (идея 199).
            context.Skip($"blacklist:{messageType}");

            var envelope = context.Envelope;
            BusTelemetry.Blacklisted(messageType, envelope.MessageId.ToString("N"), "on the fly (идея 349)");
            _logger.LogWarning(
                "Сообщение {MessageId} ({MessageType}) отклонено чёрным списком",
                envelope.MessageId,
                messageType);

            return ValueTask.CompletedTask;
        }

        return next(context);
    }
}
