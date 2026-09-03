using System.Text.Json;
using System.Text.Json.Serialization;
using AvtoBus.Configuration;
using AvtoBus.Handlers;

namespace AvtoBus.AsyncApi;

/// <summary>Метаданные документа AsyncAPI.</summary>
public sealed class AsyncApiInfo
{
    public string Title { get; set; } = "AvtoBus API";
    public string Version { get; set; } = "1.0.0";
    public string Description { get; set; } = "";
    public Dictionary<string, object> Servers { get; set; } = new();
}

/// <summary>
/// Генерирует AsyncAPI 3.0 спецификацию из compile-time модели шины:
/// хендлеры диспетчера + маршруты (очереди команд / топики событий) + схемы контрактов.
/// Готовый JSON можно отдавать по <c>/asyncapi.json</c> и кормить в генераторы клиентов.
/// </summary>
public sealed class AsyncApiGenerator
{
    private readonly DispatcherRegistry _dispatchers;
    private readonly RoutingTable _router;
    private readonly AsyncApiInfo _info;

    public AsyncApiGenerator(DispatcherRegistry dispatchers, RoutingTable router, AsyncApiInfo info)
    {
        _dispatchers = dispatchers;
        _router = router;
        _info = info;
    }

    /// <summary>Типы сообщений, для которых есть хендлеры (без дубликатов).</summary>
    public IEnumerable<Type> MessageTypes => _dispatchers.HandledTypes.OrderBy(t => t.FullName, StringComparer.Ordinal);

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Генерация схем сканирует свойства контрактов через рефлексию — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Генерация документа сериализует произвольные контракты через reflection-STJ — несовместимо с NativeAOT.")]
    public string Generate()
    {
        var doc = new Dictionary<string, object>
        {
            ["asyncapi"] = "3.0.0",
            ["info"] = new Dictionary<string, object>
            {
                ["title"] = _info.Title,
                ["version"] = _info.Version,
                ["description"] = _info.Description,
            },
            ["defaultContentType"] = "application/json",
            ["servers"] = _info.Servers,
            ["channels"] = BuildChannels(),
            ["operations"] = BuildOperations(),
            ["components"] = new Dictionary<string, object>
            {
                ["messages"] = BuildMessages(),
                ["schemas"] = BuildSchemas(),
            },
        };

        return JsonSerializer.Serialize(doc, Options);
    }

    private Dictionary<string, object> BuildChannels()
    {
        var channels = new Dictionary<string, object>();
        foreach (var type in MessageTypes)
        {
            var (channelName, kind) = RouteOf(type);
            var messageKey = MessageKey(type);

            if (channels.TryGetValue(channelName, out var existing)
                && existing is Dictionary<string, object> existingDict
                && existingDict["messages"] is Dictionary<string, object> existingMessages)
            {
                existingMessages[messageKey] = new Dictionary<string, object>
                {
                    ["$ref"] = $"#/components/messages/{SanitizeRef(messageKey)}",
                };
            }
            else
            {
                channels[channelName] = new Dictionary<string, object>
                {
                    ["address"] = channelName,
                    ["messages"] = new Dictionary<string, object>
                    {
                        [messageKey] = new Dictionary<string, object>
                        {
                            ["$ref"] = $"#/components/messages/{SanitizeRef(messageKey)}",
                        },
                    },
                };
            }
        }
        return channels;
    }

    private Dictionary<string, object> BuildOperations()
    {
        var operations = new Dictionary<string, object>();
        foreach (var type in MessageTypes)
        {
            var (channelName, kind) = RouteOf(type);
            var safeChannel = SanitizeRef(channelName);

            operations[$"{ActionOf(kind)}_{SanitizeRef(MessageKey(type))}"] = new Dictionary<string, object>
            {
                ["action"] = ActionOf(kind),
                ["channel"] = new Dictionary<string, object>
                {
                    ["$ref"] = $"#/channels/{safeChannel}",
                },
                ["summary"] = $"{(kind is DestinationKind.Queue ? "Consumes command" : "Consumes event")} {type.Name}",
                ["messages"] = new List<object>
                {
                    new Dictionary<string, object>
                    {
                        ["$ref"] = $"#/components/messages/{SanitizeRef(MessageKey(type))}",
                    },
                },
            };
        }
        return operations;
    }

    private Dictionary<string, object> BuildMessages()
    {
        var messages = new Dictionary<string, object>();
        foreach (var type in MessageTypes)
        {
            var messageKey = MessageKey(type);
            messages[SanitizeRef(messageKey)] = new Dictionary<string, object>
            {
                ["name"] = messageKey,
                ["title"] = type.Name,
                ["contentType"] = "application/json",
                ["payload"] = new Dictionary<string, object>
                {
                    ["$ref"] = $"#/components/schemas/{SanitizeRef(type.Name)}",
                },
            };
        }
        return messages;
    }

    private Dictionary<string, object> BuildSchemas()
    {
        var schemas = new Dictionary<string, object>();
        foreach (var type in MessageTypes)
        {
            schemas[SanitizeRef(type.Name)] = BuildSchema(type);
        }
        return schemas;
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2070", Justification =
        "Вызывается только из Generate (аннотирован RUC): типы контрактов сохраняет приложение.")]
    private static object BuildSchema(Type type)
    {
        var properties = new Dictionary<string, object>();
        foreach (var prop in type.GetProperties())
        {
            if (prop.GetIndexParameters().Length > 0)
                continue;

            properties[prop.Name] = new Dictionary<string, object>
            {
                ["type"] = MapClrToJsonType(prop.PropertyType),
                ["description"] = prop.DescriptionFromXmlDocs(),
            };
        }
        return new Dictionary<string, object>
        {
            ["type"] = "object",
            ["properties"] = properties,
        };
    }

    private (string Channel, DestinationKind Kind) RouteOf(Type type)
    {
        var isCommand = typeof(ICommand).IsAssignableFrom(type);
        var kind = isCommand ? OutgoingKind.Send : OutgoingKind.Publish;
        var route = _router.Resolve(type, kind);
        return (route.Destination.Name, route.Destination.Kind);
    }

    private static string ActionOf(DestinationKind kind)
        => kind is DestinationKind.Queue ? "receive" : "receive";

    private static string MessageKey(Type type) => MessageTypeNaming.NameOf(type);

    private static string MapClrToJsonType(Type t)
    {
        var underlying = Nullable.GetUnderlyingType(t) ?? t;
        if (underlying == typeof(string) || underlying == typeof(Guid) || underlying == typeof(char))
            return "string";
        if (underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset) || underlying == typeof(TimeSpan))
            return "string";
        if (underlying == typeof(int) || underlying == typeof(long) || underlying == typeof(short) || underlying == typeof(byte))
            return "integer";
        if (underlying == typeof(decimal) || underlying == typeof(double) || underlying == typeof(float))
            return "number";
        if (underlying == typeof(bool))
            return "boolean";
        if (underlying.IsEnum)
            return "string";
        if (underlying.IsArray || (underlying.IsGenericType && typeof(System.Collections.IEnumerable).IsAssignableFrom(underlying)))
            return "array";
        return "object";
    }

    private static string SanitizeRef(string s) => s.Replace(".", "_", StringComparison.Ordinal).Replace("/", "_", StringComparison.Ordinal).Replace("#", "_", StringComparison.Ordinal).Replace("*", "_", StringComparison.Ordinal).Replace(":", "_", StringComparison.Ordinal);

    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
}

internal static class PropertyInfoXmlDocs
{
    public static string DescriptionFromXmlDocs(this System.Reflection.PropertyInfo prop)
    {
        var attr = prop.GetCustomAttributes(true)
            .OfType<System.ComponentModel.DescriptionAttribute>()
            .FirstOrDefault();
        return attr?.Description ?? "";
    }
}
