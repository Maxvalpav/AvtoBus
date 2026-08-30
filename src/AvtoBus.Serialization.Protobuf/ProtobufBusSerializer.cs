using System.Buffers;
using System.Reflection;
using AvtoBus.Configuration;
using AvtoBus.Serialization;
using Google.Protobuf;

namespace AvtoBus.Serialization.Protobuf;

/// <summary>
/// Protobuf-сериализация (идея 105) для контрактов, сгенерированных из .proto
/// (<see cref="IMessage{T}"/>). Схема жёстко задана .proto-файлом — контракт версионируется
/// через номера полей, а не через миграцию типов.
/// </summary>
/// <remarks>
/// Десериализация идёт через статический <c>Parser</c> типа (генерируется protoc), с фолбэком
/// на <see cref="IMessage.MergeFrom"/> для самописных реализаций <see cref="IMessage"/>.
/// </remarks>
public sealed class ProtobufBusSerializer : IMessageSerializer
{
    public string ContentType => "application/x-protobuf";

    public void Serialize(IBufferWriter<byte> writer, object message, Type type)
    {
        if (message is not IMessage proto)
            throw new NotSupportedException(
                $"Тип {type.Name} не является Google.Protobuf IMessage. Контракты для protobuf " +
                "генерируются из .proto-файлов (protoc / Grpc.Tools).");

        writer.Write(proto.ToByteArray());
    }

    public object? Deserialize(ReadOnlyMemory<byte> body, Type type)
    {
        if (!typeof(IMessage).IsAssignableFrom(type))
            throw new NotSupportedException(
                $"Тип {type.Name} не является Google.Protobuf IMessage. Контракты для protobuf " +
                "генерируются из .proto-файлов (protoc / Grpc.Tools).");

        // Сгенерированные protoc-типы имеют статический Parser — используем его.
        var parser = FindParser(type);
        if (parser is not null)
            return InvokeParse(parser, body);

        // Фолбэк: самописный IMessage — создаём экземпляр и мерджим байты.
        var instance = (IMessage)Activator.CreateInstance(type)!;
        instance.MergeFrom(body.ToArray());
        return instance;
    }

    private static object? FindParser(Type type)
        => type.GetProperty("Parser", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);

    private static object? InvokeParse(object parser, ReadOnlyMemory<byte> body)
    {
        var parse = parser.GetType().GetMethod("ParseFrom", [typeof(byte[])])
                    ?? throw new NotSupportedException($"Parser типа {parser.GetType().Name} не имеет ParseFrom(byte[]).");
        return parse.Invoke(parser, [body.ToArray()]);
    }
}

/// <summary>Включение Protobuf как дефолтного сериализатора шины (идея 105).</summary>
public static class ProtobufSerializationExtensions
{
    /// <summary>
    /// Ставит Protobuf сериализатором по умолчанию: контракты на проводе идут в
    /// <c>application/x-protobuf</c>, приём распознаёт формат по <c>Content-Type</c>.
    /// </summary>
    public static BusConfigurator UseProtobuf(this BusConfigurator bus)
        => bus.Serialization(s => s.SetDefault(new ProtobufBusSerializer()));
}
