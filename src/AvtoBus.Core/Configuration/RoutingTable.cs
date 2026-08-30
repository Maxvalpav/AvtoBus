using System.Collections.Concurrent;

namespace AvtoBus.Configuration;

/// <summary>
/// Решает, куда уходит сообщение. По умолчанию — конвенция:
/// команда → очередь по kebab-case имени, событие → топик по имени контракта.
/// </summary>
public sealed class RoutingTable
{
    private readonly Dictionary<Type, RouteEntry> _explicitRoutes = [];
    private readonly List<(Func<Type, bool> Predicate, RouteEntry Route)> _rules = [];
    private readonly ConcurrentDictionary<(Type, OutgoingKind), RouteEntry> _cache = new();

    public void MapCommand(Type messageType, string queue, string? transport = null)
        => _explicitRoutes[messageType] = new RouteEntry(TransportDestination.Queue(queue), transport);

    public void MapEvent(Type messageType, string topic, string? transport = null)
        => _explicitRoutes[messageType] = new RouteEntry(TransportDestination.Topic(topic), transport);

    /// <summary>Правило для группы типов: например, всё из namespace <c>Analytics</c> — в Kafka.</summary>
    public void MapRule(Func<Type, bool> predicate, TransportDestination destination, string? transport = null)
        => _rules.Add((predicate, new RouteEntry(destination, transport)));

    /// <summary>Направляет все типы, подходящие под предикат, в указанный транспорт, не меняя назначения.</summary>
    public void MapTransport(Func<Type, bool> predicate, string transport)
        => _rules.Add((predicate, new RouteEntry(default, transport)));

    public RouteEntry Resolve(Type messageType, OutgoingKind kind)
        => _cache.GetOrAdd((messageType, kind), key => ResolveCore(key.Item1, key.Item2));

    private RouteEntry ResolveCore(Type messageType, OutgoingKind kind)
    {
        if (_explicitRoutes.TryGetValue(messageType, out var route))
            return route;

        string? transport = null;
        foreach (var (predicate, rule) in _rules)
        {
            if (!predicate(messageType))
                continue;

            // Правило без назначения задаёт только транспорт — назначение остаётся конвенционным.
            if (rule.Destination.Name is not null)
                return rule with { Transport = rule.Transport ?? transport };

            transport = rule.Transport;
        }

        return new RouteEntry(Conventional(messageType, kind), transport);
    }

    /// <summary>
    /// Конвенция: команда идёт в очередь (один владелец), событие — в топик (fan-out).
    /// </summary>
    public static TransportDestination Conventional(Type messageType, OutgoingKind kind)
    {
        var name = MessageTypeNaming.NameOf(messageType);

        return kind is OutgoingKind.Send && IsCommand(messageType)
            ? TransportDestination.Queue(CommandQueueName(messageType))
            : TransportDestination.Topic(name);
    }

    /// <summary>Имя очереди команды: только имя типа в kebab-case, без namespace-префикса.</summary>
    public static string CommandQueueName(Type messageType)
    {
        if (messageType.GetCustomAttributes(typeof(TopicAttribute), false) is [TopicAttribute topic])
            return topic.Name;

        return MessageTypeNaming.ToKebabCase(messageType.Name);
    }

    private static bool IsCommand(Type type) => typeof(ICommand).IsAssignableFrom(type);
}

/// <param name="Destination">Куда отправлять. <c>default</c> означает «по конвенции».</param>
/// <param name="Transport">Имя транспорта или <c>null</c> для транспорта по умолчанию.</param>
public readonly record struct RouteEntry(TransportDestination Destination, string? Transport);

/// <summary>Fluent-конфигуратор правил маршрутизации.</summary>
public sealed class RouteConfigurator(RoutingTable table)
{
    public CommandRoute<T> Command<T>() where T : class => new(table);

    public EventRoute<T> Event<T>() where T : class => new(table);

    /// <summary>Правило для всех событий из namespace.</summary>
    public NamespaceRoute Events() => new(table, static _ => true);

    public sealed class CommandRoute<T>(RoutingTable table) where T : class
    {
        public CommandRoute<T> ToQueue(string queue)
        {
            table.MapCommand(typeof(T), queue);
            return this;
        }

        public CommandRoute<T> Via(string transport)
        {
            table.MapTransport(type => type == typeof(T), transport);
            return this;
        }
    }

    public sealed class EventRoute<T>(RoutingTable table) where T : class
    {
        public EventRoute<T> ToTopic(string topic)
        {
            table.MapEvent(typeof(T), topic);
            return this;
        }

        public EventRoute<T> Via(string transport)
        {
            table.MapTransport(type => type == typeof(T), transport);
            return this;
        }
    }

    public sealed class NamespaceRoute(RoutingTable table, Func<Type, bool> predicate)
    {
        private Func<Type, bool> _predicate = predicate;

        public NamespaceRoute FromNamespace(string @namespace)
        {
            var previous = _predicate;
            _predicate = type => previous(type)
                                 && type.Namespace is { } ns
                                 && ns.StartsWith(@namespace, StringComparison.Ordinal);
            return this;
        }

        public NamespaceRoute ToTopic(string topic)
        {
            table.MapRule(_predicate, TransportDestination.Topic(topic));
            return this;
        }

        public NamespaceRoute Via(string transport)
        {
            table.MapTransport(_predicate, transport);
            return this;
        }
    }
}
