using Microsoft.Extensions.Logging;

namespace AvtoBus.Scheduling;

/// <summary>
/// Реестр cron-джобов, объявленных в коде (идея 223).
/// </summary>
public interface ICronRegistry
{
    void Add<TMessage>(string name, string cronExpression, TMessage payload,
        string timeZoneId = "UTC", MisfirePolicy misfire = MisfirePolicy.FireLatest)
        where TMessage : class;

    IReadOnlyList<CronRegistration> Registrations { get; }
}

public sealed record CronRegistration(
    string Name,
    string CronExpression,
    string TimeZoneId,
    object Payload,
    Type PayloadType,
    MisfirePolicy Misfire);

internal sealed class CronRegistry : ICronRegistry
{
    private readonly List<CronRegistration> _registrations = new();

    public IReadOnlyList<CronRegistration> Registrations => _registrations;

    public void Add<TMessage>(string name, string cronExpression, TMessage payload,
        string timeZoneId = "UTC", MisfirePolicy misfire = MisfirePolicy.FireLatest)
        where TMessage : class
    {
        CronExpression.Parse(cronExpression);
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Cron job name cannot be empty", nameof(name));

        _registrations.Add(new CronRegistration(
            name, cronExpression, timeZoneId, payload, typeof(TMessage), misfire));
    }
}
