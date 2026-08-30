using System.Buffers;
using AvtoBus.Configuration;
using AvtoBus.Serialization;
using MessagePack;

namespace AvtoBus.Serialization.MessagePack;

/// <summary>
/// MessagePack-сериализация (идея 105): компактнее и быстрее JSON, с LZ4-компрессией по умолчанию.
/// Работает с тем же <see cref="SerializerRegistry"/>, что и JSON: приёмник выбирает сериализатор
/// по <c>Content-Type</c> конверта, поэтому один консьюмер принимает и JSON, и MessagePack.
/// </summary>
/// <remarks>
/// Контракты — POCO: MessagePack сериализует публичные свойства по конвенции. Для точного контроля
/// схемы добавляйте <c>[MessagePackObject]</c>/<c>[Key]</c>-атрибуты (потребуется keyAsPropertyName
/// в <see cref="MessagePackSerializerOptions"/> для совместимости с wire-протоколом шины).
/// </remarks>
public sealed class MessagePackBusSerializer : IMessageSerializer
{
    /// <summary>Дефолтные опции: contractless (POCO без атрибутов). Сжатие — опционально через WithCompression.</summary>
    public static readonly MessagePackSerializerOptions DefaultOptions =
        MessagePackSerializerOptions.Standard
            .WithResolver(global::MessagePack.Resolvers.ContractlessStandardResolver.Instance);

    private readonly MessagePackSerializerOptions _options;

    public MessagePackBusSerializer(MessagePackSerializerOptions? options = null)
        => _options = options ?? DefaultOptions;

    public string ContentType => "application/x-msgpack";

    public void Serialize(IBufferWriter<byte> writer, object message, Type type)
    {
        MessagePackSerializer.Serialize(type, writer, message, _options);
    }

    public object? Deserialize(ReadOnlyMemory<byte> body, Type type)
        => MessagePackSerializer.Deserialize(type, body, _options);
}

/// <summary>Включение MessagePack как дефолтного сериализатора шины (идея 105).</summary>
public static class MessagePackSerializationExtensions
{
    /// <summary>
    /// Ставит MessagePack сериализатором по умолчанию: контракты на проводе идут в
    /// <c>application/x-msgpack</c>, приём распознаёт формат по <c>Content-Type</c>.
    /// </summary>
    public static BusConfigurator UseMessagePack(this BusConfigurator bus, MessagePackSerializerOptions? options = null)
        => bus.Serialization(s => s.SetDefault(new MessagePackBusSerializer(options)));
}
