namespace AvtoBus.Runtime;

/// <summary>
/// Транспорты, доступные приложению. Мульти-транспорт — норма: команды в RabbitMQ,
/// аналитика в Kafka, уведомления в Redis (идея 73).
/// </summary>
public sealed class TransportRegistry(IEnumerable<ITransport> transports, string defaultTransport)
{
    private readonly Dictionary<string, ITransport> _transports = Build(transports);

    private static Dictionary<string, ITransport> Build(IEnumerable<ITransport> transports)
    {
        var dict = new Dictionary<string, ITransport>(StringComparer.OrdinalIgnoreCase);
        foreach (var t in transports)
        {
            if (!dict.TryAdd(t.Name, t))
                throw new InvalidOperationException($"Дубликат транспорта '{t.Name}': зарегистрирован дважды. Проверьте Use* вызовы.");
        }
        return dict;
    }

    public ITransport Default => Get(defaultTransport);

    public IEnumerable<ITransport> All => _transports.Values;

    public ITransport Get(string? name)
    {
        if (name is null)
            return _transports.TryGetValue(defaultTransport, out var fallback)
                ? fallback
                : throw new InvalidOperationException(
                    $"Транспорт по умолчанию '{defaultTransport}' не зарегистрирован. " +
                    "Вызовите bus.UseTransport(...) при настройке.");

        return _transports.TryGetValue(name, out var transport)
            ? transport
            : throw new InvalidOperationException(
                $"Транспорт '{name}' не зарегистрирован. Доступны: {string.Join(", ", _transports.Keys)}.");
    }
}
