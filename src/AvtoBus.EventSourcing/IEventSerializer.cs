using System.Text.Json;

namespace AvtoBus.EventSourcing;

/// <summary>Сериализация событий и снапшотов в байты (идея 251).</summary>
public interface IEventSerializer
{
    ReadOnlyMemory<byte> Serialize(object @event);
    object Deserialize(ReadOnlyMemory<byte> data, string eventType);
    ReadOnlyMemory<byte> SerializeSnapshot(object state);
    T DeserializeSnapshot<T>(ReadOnlyMemory<byte> data) where T : class;
    void RegisterType(string eventType, Type clrType);

    /// <summary>CLR-тип по имени типа события; null, если не зарегистрирован.</summary>
    Type? ResolveType(string eventType);
}

public sealed class JsonEventSerializer : IEventSerializer
{
    private readonly Dictionary<string, Type> _typeMap = new(StringComparer.OrdinalIgnoreCase);
    private readonly JsonSerializerOptions _options;

    public JsonEventSerializer(IEnumerable<Type> eventTypes, JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        foreach (var t in eventTypes)
            _typeMap[MessageTypeNaming.NameOf(t)] = t;
    }

    public void RegisterType(string eventType, Type clrType) => _typeMap[eventType] = clrType;

    public Type? ResolveType(string eventType)
        => _typeMap.TryGetValue(eventType, out var clrType) ? clrType : null;

    /// <summary>
    /// Дефолтный сериализатор событий через reflection-STJ. Типы событий регистрирует
    /// явно приложение; под строгим AOT подмените на source-generated IEventSerializer.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Типы событий регистрирует явно приложение; под строгим AOT — свой IEventSerializer.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Типы событий регистрирует явно приложение; под строгим AOT — свой IEventSerializer.")]
    public ReadOnlyMemory<byte> Serialize(object @event)
        => JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), _options);

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Типы событий регистрирует явно приложение; под строгим AOT — свой IEventSerializer.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Типы событий регистрирует явно приложение; под строгим AOT — свой IEventSerializer.")]
    public object Deserialize(ReadOnlyMemory<byte> data, string eventType)
    {
        if (!_typeMap.TryGetValue(eventType, out var clrType))
            throw new UnknownEventTypeException(eventType);

        return JsonSerializer.Deserialize(data.Span, clrType, _options)
            ?? throw new InvalidOperationException($"Null after deserializing {eventType}");
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Снапшоты персистят произвольное состояние: типы сохраняет приложение.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Снапшоты персистят произвольное состояние: типы сохраняет приложение.")]
    public ReadOnlyMemory<byte> SerializeSnapshot(object state)
        => JsonSerializer.SerializeToUtf8Bytes(state, state.GetType(), _options);

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Снапшоты персистят произвольное состояние: типы сохраняет приложение.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Снапшоты персистят произвольное состояние: типы сохраняет приложение.")]
    public T DeserializeSnapshot<T>(ReadOnlyMemory<byte> data) where T : class
        => JsonSerializer.Deserialize<T>(data.Span, _options)!;
}

public sealed class UnknownEventTypeException(string eventType)
    : Exception($"Unknown event type '{eventType}'. Register it in JsonEventSerializer.");
