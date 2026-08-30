namespace AvtoBus.Runtime;

/// <summary>
/// Транспорты, доступные приложению. Мульти-транспорт — норма: команды в RabbitMQ,
/// аналитика в Kafka, уведомления в Redis (идея 73).
/// </summary>
public sealed class TransportRegistry(IEnumerable<ITransport> transports, string defaultTransport)
{
    private readonly Dictionary<string, ITransport> _transports =
        transports.ToDictionary(t => t.Name, StringComparer.OrdinalIgnoreCase);

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
