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

        // Write via byte array (CodedOutputStream requires byte[] in this version)
        var bytes = proto.ToByteArray();
        writer.Write(bytes);
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
        if (Activator.CreateInstance(type) is not IMessage instance)
            throw new NotSupportedException($"Тип {type.Name} не имеет публичного конструктора без параметров");
        instance.MergeFrom(new Google.Protobuf.CodedInputStream(body.ToArray()));
        return instance;
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, (object Parser, MethodInfo ParseMethod)> ParserCache = new();

    private static object? FindParser(Type type)
    {
        if (ParserCache.TryGetValue(type, out var cached))
            return cached.Parser;
        var parser = type.GetProperty("Parser", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);
        if (parser is null) return null;
        var parse = parser.GetType().GetMethod("ParseFrom", [typeof(byte[])])
                    ?? throw new NotSupportedException($"Parser типа {parser.GetType().Name} не имеет ParseFrom(byte[]).");
        ParserCache[type] = (parser, parse);
        return parser;
    }

    private static object? InvokeParse(object parser, ReadOnlyMemory<byte> body)
    {
        // Direct lookup: parser type is unique per message type, find cache entry by parser reference
        foreach (var kv in ParserCache)
        {
            if (ReferenceEquals(kv.Value.Parser, parser))
                return kv.Value.ParseMethod.Invoke(parser, [body.ToArray()]);
        }
        // Fallback: resolve via parser's declaring type
        var parserType = parser.GetType();
        var parse = parserType.GetMethod("ParseFrom", [typeof(byte[])])
                     ?? parserType.GetMethod("ParseFrom", [typeof(Google.Protobuf.CodedInputStream)])
                     ?? throw new NotSupportedException($"Parser типа {parserType.Name} не имеет ParseFrom.");
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
