using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace AvtoBus.Serialization;

/// <summary>
/// Сериализатор тела сообщения. Пишет напрямую в <see cref="IBufferWriter{T}"/> — без промежуточных
/// массивов на горячем пути (идея 355).
/// </summary>
public interface IMessageSerializer
{
    /// <summary>Значение заголовка <c>Content-Type</c>, по которому выбирается десериализатор на приёме.</summary>
    string ContentType { get; }

    void Serialize(IBufferWriter<byte> writer, object message, Type type);

    object? Deserialize(ReadOnlyMemory<byte> body, Type type);
}

/// <summary>
/// System.Text.Json — дефолт. Работает и с source-generated контекстом (AOT), и с рефлексией.
/// </summary>
/// <remarks>
/// Рефлексия — ограниченный legacy-режим (док 01, §«Рефлексия vs codegen»): если контракт не покрыт
/// <see cref="JsonSerializerContext"/>, сериализация несовместима с trimming/AOT. Для AOT зарегистрируйте
/// source-generated контекст через <c>BusConfigurator.UseJsonSerializerContext</c> — тогда сериализация
/// идёт через <see cref="JsonTypeInfo"/> без рефлексии.
/// </remarks>
[RequiresUnreferencedCode(
    "JsonMessageSerializer использует reflection-STJ, когда тип не покрыт JsonSerializerContext. " +
    "Для AOT регистрируйте контекст через BusConfigurator.UseJsonSerializerContext.")]
public sealed class JsonMessageSerializer : IMessageSerializer
{
    public static readonly JsonSerializerOptions DefaultOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true,
    };

    private readonly JsonSerializerOptions? _options;
    private readonly JsonSerializerContext? _context;

    public JsonMessageSerializer(JsonSerializerOptions? options = null)
        : this(null, options)
    {
    }

    /// <summary>
    /// Сериализатор с source-generated контекстом: типы, покрытые контекстом, идут через
    /// <see cref="JsonTypeInfo"/> (AOT-safe); прочие — через рефлексию.
    /// </summary>
    public JsonMessageSerializer(JsonSerializerContext? context, JsonSerializerOptions? options = null)
    {
        _context = context;
        _options = options ?? DefaultOptions;
    }

    public string ContentType => "application/json";

    public void Serialize(IBufferWriter<byte> writer, object message, Type type)
    {
        using var json = new Utf8JsonWriter(writer);

        // AOT-safe путь: тип известен контексту статически.
        if (_context is { } context && context.GetTypeInfo(type) is { } info)
        {
            JsonSerializer.Serialize(json, message, info);
            return;
        }

        JsonSerializer.Serialize(json, message, type, _options);
    }

    public object? Deserialize(ReadOnlyMemory<byte> body, Type type)
    {
        if (_context is { } context && context.GetTypeInfo(type) is { } info)
            return JsonSerializer.Deserialize(body.Span, info);

        return JsonSerializer.Deserialize(body.Span, type, _options);
    }
}

/// <summary>
/// Выбирает сериализатор на отправке и — по <c>ContentType</c> конверта — на приёме.
/// Позволяет одному консьюмеру принимать и JSON, и MessagePack (идея 106).
/// </summary>
public sealed class SerializerRegistry
{
    private readonly Dictionary<string, IMessageSerializer> _byContentType = new(StringComparer.OrdinalIgnoreCase);

    public SerializerRegistry(IMessageSerializer @default)
    {
        Default = @default;
        Register(@default);
    }

    public IMessageSerializer Default { get; private set; }

    public void Register(IMessageSerializer serializer) => _byContentType[serializer.ContentType] = serializer;

    public void SetDefault(IMessageSerializer serializer)
    {
        Register(serializer);
        Default = serializer;
    }

    /// <summary>
    /// Ищет сериализатор по content-type конверта. Параметры вида <c>; charset=utf-8</c> отбрасываются.
    /// Неизвестный тип — ошибка, а не тихий фолбэк на дефолт: иначе получим мусор вместо сообщения.
    /// </summary>
    public IMessageSerializer For(string contentType)
    {
        var semicolon = contentType.IndexOf(';');
        var media = semicolon >= 0 ? contentType[..semicolon].Trim() : contentType.Trim();

        return _byContentType.TryGetValue(media, out var serializer)
            ? serializer
            : throw new NotSupportedException(
                $"Нет сериализатора для content-type '{media}'. Зарегистрированы: {string.Join(", ", _byContentType.Keys)}.");
    }
}
