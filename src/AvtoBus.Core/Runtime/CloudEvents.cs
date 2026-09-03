using System.Collections.Frozen;
using System.Globalization;

namespace AvtoBus.Runtime;

/// <summary>
/// CloudEvents 1.0 (идея 117): конверт AvtoBus маппится в атрибуты CloudEvents бинарного режима
/// (ce-* заголовки). Любой совместимый потребитель читает конверт как CloudEvent, тело — как data.
///
/// Обратный путь не нужен: входящий конверт уже содержит все заголовки, AvtoBus читает свои
/// (MessageId, MessageType) из собственных полей, а не из ce-*.
/// </summary>
public static class CloudEvents
{
    /// <summary>Версия спецификации: конверт помечается как CloudEvents 1.0.</summary>
    public const string SpecVersionHeader = "ce-specversion";

    public const string IdHeader = "ce-id";

    /// <summary>Тип события — имя контракта AvtoBus (orders.order-placed.v1).</summary>
    public const string TypeHeader = "ce-type";

    /// <summary>Источник события — имя сервиса-отправителя (source).</summary>
    public const string SourceHeader = "ce-source";

    /// <summary>Момент отправки в RFC 3339 — время события.</summary>
    public const string TimeHeader = "ce-time";

    public const string SpecVersion = "1.0";

    /// <summary>
    /// Проставляет обязательные атрибуты CloudEvents в заголовки конверта. Существующие ce-*
    /// заголовки (например, заданные приложением) не перезаписываются.
    /// </summary>
    public static Envelope Apply(Envelope envelope, string source)
    {
        var headers = new Dictionary<string, string>(envelope.Headers, StringComparer.Ordinal);

        headers.TryAdd(SpecVersionHeader, SpecVersion);
        headers.TryAdd(IdHeader, envelope.MessageId.ToString("D"));
        headers.TryAdd(TypeHeader, envelope.MessageType);
        headers.TryAdd(SourceHeader, source);
        headers.TryAdd(TimeHeader, envelope.SentAt.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));

        return envelope with { Headers = headers.ToFrozenDictionary(StringComparer.Ordinal) };
    }
}
