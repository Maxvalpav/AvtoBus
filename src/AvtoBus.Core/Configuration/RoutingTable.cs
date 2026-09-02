using System.Collections.Concurrent;

namespace AvtoBus.Configuration;

/// <summary>
/// Решает, куда уходит сообщение. По умолчанию — конвенция:
/// команда → очередь по kebab-case имени, событие → топик по имени контракта.
/// </summary>
public sealed class RoutingTable
{
    internal readonly Dictionary<Type, RouteEntry> _explicitRoutes = [];
    internal readonly List<(Func<Type, bool> Predicate, RouteEntry Route)> _rules = [];
    private readonly ConcurrentDictionary<(Type, OutgoingKind), RouteEntry> _cache = new();

    public void MapCommand(Type messageType, string queue, string? transport = null)
    {
        if (_explicitRoutes.TryGetValue(messageType, out var existing) && transport is null) transport = existing.Transport;
        _explicitRoutes[messageType] = new RouteEntry(TransportDestination.Queue(queue), transport);
        _cache.Clear();
    }

    public void MapEvent(Type messageType, string topic, string? transport = null)
    {
        if (_explicitRoutes.TryGetValue(messageType, out var existing) && transport is null) transport = existing.Transport;
        _explicitRoutes[messageType] = new RouteEntry(TransportDestination.Topic(topic), transport);
        _cache.Clear();
    }

    /// <summary>Правило для группы типов: например, всё из namespace <c>Analytics</c> — в Kafka.</summary>
    public void MapRule(Func<Type, bool> predicate, TransportDestination destination, string? transport = null)
    { _rules.Add((predicate, new RouteEntry(destination, transport))); _cache.Clear(); }

    /// <summary>Направляет все типы, подходящие под предикат, в указанный транспорт, не меняя назначения.</summary>
    public void MapTransport(Func<Type, bool> predicate, string transport)
    { _rules.Add((predicate, new RouteEntry(default, transport))); _cache.Clear(); }

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

    private static bool IsCommand(Type type)
    {
        var isCmd = typeof(ICommand).IsAssignableFrom(type);
        var isEvt = typeof(IEvent).IsAssignableFrom(type);
        if (isCmd && isEvt) return false; // тип-«хамелеон» → по умолчанию топик (fan-out), не очередь
        return isCmd;
    }
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
            // Preserve already-set transport via Via() if called first
            var existing = table._explicitRoutes.TryGetValue(typeof(T), out var e) ? e.Transport : null;
            if (table._explicitRoutes.ContainsKey(typeof(T)) && table._rules.Any(r => r.Predicate(typeof(T))))
            {
                // Via was called first as MapTransport — migrate it into explicit route
                var viaTransport = table._rules.Where(r => r.Predicate(typeof(T))).Select(r => r.Route.Transport).LastOrDefault(t => t != null);
                if (viaTransport != null) table._rules.RemoveAll(r => r.Predicate(typeof(T)) && r.Route.Destination.Name is null);
                table.MapCommand(typeof(T), queue, viaTransport ?? existing);
            }
            else table.MapCommand(typeof(T), queue);
            return this;
        }

        public CommandRoute<T> Via(string transport)
        {
            if (table._explicitRoutes.TryGetValue(typeof(T), out var existing))
                table._explicitRoutes[typeof(T)] = existing with { Transport = transport };
            else
                table.MapTransport(type => type == typeof(T), transport);
            return this;
        }
    }

    public sealed class EventRoute<T>(RoutingTable table) where T : class
    {
        public EventRoute<T> ToTopic(string topic)
        {
            var existing = table._explicitRoutes.TryGetValue(typeof(T), out var e) ? e.Transport : null;
            if (table._explicitRoutes.ContainsKey(typeof(T)) && table._rules.Any(r => r.Predicate(typeof(T))))
            {
                var viaTransport = table._rules.Where(r => r.Predicate(typeof(T))).Select(r => r.Route.Transport).LastOrDefault(t => t != null);
                if (viaTransport != null) table._rules.RemoveAll(r => r.Predicate(typeof(T)) && r.Route.Destination.Name is null);
                table.MapEvent(typeof(T), topic, viaTransport ?? existing);
            }
            else table.MapEvent(typeof(T), topic);
            return this;
        }

        public EventRoute<T> Via(string transport)
        {
            if (table._explicitRoutes.TryGetValue(typeof(T), out var existing))
                table._explicitRoutes[typeof(T)] = existing with { Transport = transport };
            else
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
            var captured = previous;
            _predicate = type => captured(type)
                                 && type.Namespace is { } ns
                                 && ns.StartsWith(@namespace, StringComparison.Ordinal);
            return this;
        }

        public NamespaceRoute ToTopic(string topic)
        {
            var snap = _predicate;
            table.MapRule(snap, TransportDestination.Topic(topic));
            return this;
        }

        public NamespaceRoute Via(string transport)
        {
            var snap = _predicate;
            table.MapTransport(snap, transport);
            return this;
        }
    }
}
