using System.Buffers;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using AvtoBus.Configuration;
using AvtoBus.Observability;
using AvtoBus.Serialization;

namespace AvtoBus.Runtime;

/// <summary>
/// Собирает конверт из сообщения и опций: сериализует тело, проставляет идентификаторы,
/// протаскивает correlation/causation и W3C-трейс.
/// </summary>
public sealed class EnvelopeFactory(BusOptions options, MessageRegistry registry, TimeProvider time)
{
    /// <summary>
    /// Создаёт конверт. <paramref name="parent"/> — конверт обрабатываемого сообщения,
    /// если это каскад: из него наследуются correlation и причинность (идея 12).
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Разбор [PartitionKey]/[MessageAttribute] — метаданные контракта через рефлексию (legacy-режим, " +
        "док 01 §codegen). Сериализация тела при зарегистрированном контексте AOT-safe.")]
    public Envelope Create(object message, Type messageType, MessageOptions? messageOptions, Envelope? parent)
    {
        var serializer = options.Serializers.Default;

        var buffer = new ArrayBufferWriter<byte>(256);
        serializer.Serialize(buffer, message, messageType);

        var messageId = messageOptions?.MessageId ?? Guid.NewGuid();

        // CorrelationId наследуется по всей цепочке: один бизнес-поток — один идентификатор.
        var correlationId = messageOptions?.CorrelationId
                            ?? parent?.CorrelationId
                            ?? parent?.MessageId
                            ?? messageId;

        // CausationId — прямой родитель: даёт дерево причинности, а не плоский список.
        var causationId = messageOptions?.CausationId ?? parent?.MessageId;

        var envelope = new Envelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            CausationId = causationId,
            MessageType = registry.NameOf(messageType),
            Body = buffer.WrittenMemory,
            ContentType = serializer.ContentType,
            SentAt = time.GetUtcNow(),
            DeliverAt = messageOptions?.DeliverAt,
            TimeToLive = messageOptions?.TimeToLive ?? TimeToLiveOf(messageType),
            PartitionKey = messageOptions?.PartitionKey ?? PartitionKeyOf(message, messageType),
            TenantId = messageOptions?.TenantId ?? parent?.TenantId ?? TenantContext.Get(),
            Priority = messageOptions?.Priority ?? parent?.Priority ?? 0,
            ReplyTo = (messageOptions as SendOptions)?.ReplyTo,
            DeliveryAttempt = 1,
            TraceParent = Activity.Current?.Id ?? parent?.TraceParent,
            Headers = BuildHeaders(messageOptions, parent, registry.NameOf(messageType)),
        };

        // Подключенная подсистема безопасности подписывает (и при включённом шифровании
        // шифрует) конверт на выходе — до транспорта (идея 451).
        return options.EnvelopeSecurity is { } security
            ? security.ProtectOutbound(envelope, InitiatorContext.Get())
            : envelope;
    }

    /// <summary>
    /// Конверт канарейки: без сериализации тела (пустой payload), с собственным MessageId.
    /// Погрешность замера не включает размер полезной нагрузки — чистый RTT транспорта (идея 337).
    /// </summary>
    public Envelope CreateForCanary(Guid messageId) => new()
    {
        MessageId = messageId,
        CorrelationId = messageId,
        MessageType = "avtobus.canary",
        Body = Array.Empty<byte>(),
        ContentType = "application/octet-stream",
        SentAt = time.GetUtcNow(),
        DeliveryAttempt = 1,
        Headers = FrozenDictionary<string, string>.Empty,
    };

    /// <summary>
    /// Заголовки каскада наследуют baggage-подобные значения родителя, но не служебные
    /// метки конкретной доставки (идея 38). Контекст ограничен по объёму и числу хопов
    /// (идея 313): длинная цепочка каскадов не «раздувает» конверт.
    /// </summary>
    private FrozenDictionary<string, string> BuildHeaders(
        MessageOptions? messageOptions,
        Envelope? parent,
        string messageTypeName)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        var truncated = false;

        if (parent is not null)
        {
            // Счётчик хопов: после MaxHops наследуемые заголовки больше не копируются.
            var hops = ReadHops(parent);
            if (hops < options.MaxHops)
            {
                headers[BusHeaders.Hops] = (hops + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);

                foreach (var (key, value) in parent.Headers)
                {
                    if (key == BusHeaders.Hops || !IsInheritable(key))
                        continue;

                    if (headers.Count >= options.MaxHeaderCount)
                    {
                        truncated = true;
                        continue;
                    }

                    headers[key] = value;
                }
            }
            else
            {
                truncated = true;
            }
        }

        if (messageOptions is not null)
        {
            foreach (var (key, value) in messageOptions.Headers)
            {
                if (headers.Count >= options.MaxHeaderCount)
                {
                    truncated = true;
                    break;
                }

                headers[key] = value;
            }
        }

        truncated |= TrimToByteLimit(headers);

        // Аудит «кто послал» (идея 332): если каскад не унаследовал инициатора,
        // а приложение задало текущего — проставляем автоматически.
        if (!headers.ContainsKey(BusHeaders.Initiator) && InitiatorContext.Get() is { } currentInitiator)
            headers[BusHeaders.Initiator] = currentInitiator;

        // Проброс пользователя (идея 454): текущий ClaimsPrincipal сериализуется в заголовок;
        // подпись добавит подключённая безопасность. Владение хопами не трогаем — безопасность
        // проверит целостность при входе.
        if (!headers.ContainsKey(BusHeaders.User) && PrincipalContext.Get() is { } principal)
            headers[BusHeaders.User] = PrincipalSerializer.Serialize(principal)!;

        if (truncated)
            BusTelemetry.HeaderTruncated(messageTypeName, "лимиты контекста (идея 313)");

        return headers.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static int ReadHops(Envelope parent)
    {
        if (parent.Header(BusHeaders.Hops) is not { } raw)
            return 0;

        return int.TryParse(raw, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var hops)
            ? hops
            : 0;
    }

    /// <summary>Сокращает суммарный объём заголовков до MaxHeaderBytes, выкидывая наибольшие значения.</summary>
    private bool TrimToByteLimit(Dictionary<string, string> headers)
    {
        var totalBytes = headers.Sum(kvp => kvp.Key.Length + kvp.Value.Length);
        if (totalBytes <= options.MaxHeaderBytes)
            return false;

        // Убираем самые «жирные» заголовки, пока не влезем в лимит — служебные выше по приоритету не ставить
        // не нужно: бюджета не хватает в принципе, Inc обсчитывать не требуется.
        foreach (var key in headers
                     .OrderByDescending(kvp => kvp.Value.Length)
                     .Select(kvp => kvp.Key)
                     .ToArray())
        {
            if (totalBytes <= options.MaxHeaderBytes)
                break;

            if (key == BusHeaders.Hops)
                continue;

            totalBytes -= key.Length + headers[key].Length;
            headers.Remove(key);
        }

        return true;
    }

    /// <summary>
    /// Служебные заголовки доставки относятся к конкретному сообщению и не должны
    /// протекать в его каскады — иначе успешное событие унаследует чужой стектрейс.
    /// </summary>
    private static bool IsInheritable(string header) => header switch
    {
        BusHeaders.ExceptionType or BusHeaders.ExceptionMessage or BusHeaders.ExceptionStackTrace => false,
        BusHeaders.FailedQueue or BusHeaders.FailedAt or BusHeaders.DeadLetterReason => false,
        BusHeaders.OriginalDestination or BusHeaders.Sequence => false,
        _ => true,
    };

    private static TimeSpan? TimeToLiveOf(Type messageType)
    {
        if (messageType.GetCustomAttributes(typeof(MessageAttribute), false) is [MessageAttribute { Ttl: { } ttl }]
            && TimeSpan.TryParse(ttl, out var parsed))
            return parsed;

        return null;
    }

    /// <summary>Достаёт ключ партиционирования из свойства, помеченного <see cref="PartitionKeyAttribute"/>.</summary>
    [RequiresUnreferencedCode("Доступ к свойству-ключу через рефлексию — legacy-режим (док 01 §codegen).")]
    private static string? PartitionKeyOf(object message, Type messageType)
    {
        var accessor = PartitionKeyAccessor.For(messageType);
        return accessor?.Invoke(message);
    }
}

/// <summary>Кэширует доступ к свойству-ключу партиции: рефлексия только при первом обращении к типу.</summary>
internal static class PartitionKeyAccessor
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<object, string?>?> Cache = new();

    [RequiresUnreferencedCode("Поиск свойства [PartitionKey] и компиляция доступа — reflection (legacy).")]
    public static Func<object, string?>? For(Type type) => Cache.GetOrAdd(type, static t =>
    {
        var property = t.GetProperties()
            .FirstOrDefault(p => p.GetCustomAttributes(typeof(PartitionKeyAttribute), true).Length > 0);

        if (property is null)
            return null;

        var instance = System.Linq.Expressions.Expression.Parameter(typeof(object), "message");
        var converted = System.Linq.Expressions.Expression.Convert(instance, t);
        var prop = System.Linq.Expressions.Expression.Property(converted, property);
        var propAsObject = System.Linq.Expressions.Expression.Convert(prop, typeof(object));
        var nullCheck = System.Linq.Expressions.Expression.Condition(
            System.Linq.Expressions.Expression.Equal(prop, System.Linq.Expressions.Expression.Constant(null, property.PropertyType)),
            System.Linq.Expressions.Expression.Constant(null, typeof(string)),
            System.Linq.Expressions.Expression.Call(propAsObject, typeof(object).GetMethod(nameof(ToString))!));

        return System.Linq.Expressions.Expression
            .Lambda<Func<object, string?>>(nullCheck, instance)
            .Compile();
    });
}
