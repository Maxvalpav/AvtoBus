using Microsoft.Extensions.Options;

namespace AvtoBus.Configuration;

/// <summary>
/// Корневая секция конфигурации AvtoBus (B5): <c>"AvtoBus"</c> в appsettings.json.
/// Биндится через <c>AddAvtoBus(IConfiguration)</c> в <see cref="AvtoBus.ServiceCollectionExtensions"/>,
/// валидируется при старте через <see cref="AvtoBusConfigValidator"/>.
/// </summary>
public sealed class AvtoBusConfiguration
{
    public const string SectionName = "AvtoBus";

    public string ServiceName { get; set; } = "avtobus";

    /// <summary>Имя транспорта по умолчанию (должен быть зарегистрирован в этом процессе).</summary>
    public string DefaultTransport { get; set; } = TransportNames.InMemory;

    public int CircuitBreakerThreshold { get; set; }

    public double CircuitBreakerDurationSeconds { get; set; } = 30;

    /// <summary>Таймаут ожидания ответа в request/response.</summary>
    public double DefaultRequestTimeoutSeconds { get; set; } = 30;

    public double ShutdownDrainTimeoutSeconds { get; set; } = 30;

    /// <summary>Лимиты контекста: объём заголовков и хопы (идея 313).</summary>
    public int MaxHeaderBytes { get; set; } = 16 * 1024;

    public int MaxHeaderCount { get; set; } = 64;

    public int MaxHops { get; set; } = 50;

    public bool PiiMaskingEnabled { get; set; }

    public bool BlacklistEnabled { get; set; }

    public List<string> InitialBlacklist { get; set; } = [];

    public bool CanaryEnabled { get; set; }

    public double CanaryIntervalSeconds { get; set; } = 30;

    public double CanaryTimeoutSeconds { get; set; } = 10;

    public double TrafficAnomalyThreshold { get; set; }

    public double TrafficAnomalyWindowSeconds { get; set; } = 60;

    public int TrafficAnomalyHistory { get; set; } = 12;

    public double InboxWindowHours { get; set; }

    public RecoverabilityConfig Recoverability { get; set; } = new();

    /// <summary>Локальные in-process очереди (идея 15): имя → ёмкость.</summary>
    public Dictionary<string, int> LocalQueues { get; set; } = [];
}

public sealed class RecoverabilityConfig
{
    public int ImmediateRetries { get; set; } = 3;

    public int DelayedRetries { get; set; } = 3;

    public double DelayedBackoffBaseSeconds { get; set; } = 5;

    public double DelayedBackoffMaxSeconds { get; set; } = 300;

    /// <summary>Что делать с сообщением, исчерпавшим попытки (enum по имени).</summary>
    public FailureAction OnFailure { get; set; } = FailureAction.MoveToErrorQueue;
}

/// <summary>
/// Ручная валидация с понятными сообщениями: собирает все ошибки сразу (идея 421),
/// срабатывает fail-fast при старте через <c>ValidateOnStart</c>.
/// </summary>
public sealed class AvtoBusConfigValidator : IValidateOptions<AvtoBusConfiguration>
{
    public ValidateOptionsResult Validate(string? name, AvtoBusConfiguration options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.ServiceName))
            errors.Add($"{AvtoBusConfiguration.SectionName}:ServiceName is required.");

        if (options.CircuitBreakerThreshold < 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:CircuitBreakerThreshold must be >= 0.");

        if (options.MaxHeaderBytes < 256)
            errors.Add($"{AvtoBusConfiguration.SectionName}:MaxHeaderBytes must be >= 256.");

        if (options.MaxHeaderCount < 1)
            errors.Add($"{AvtoBusConfiguration.SectionName}:MaxHeaderCount must be >= 1.");

        if (options.MaxHops < 1)
            errors.Add($"{AvtoBusConfiguration.SectionName}:MaxHops must be >= 1.");

        if (options.CircuitBreakerDurationSeconds <= 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:CircuitBreakerDurationSeconds must be > 0.");

        if (options.DefaultRequestTimeoutSeconds <= 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:DefaultRequestTimeoutSeconds must be > 0.");

        if (options.ShutdownDrainTimeoutSeconds < 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:ShutdownDrainTimeoutSeconds must be >= 0.");

        if (options.CanaryIntervalSeconds <= 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:CanaryIntervalSeconds must be > 0.");

        if (options.CanaryTimeoutSeconds <= 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:CanaryTimeoutSeconds must be > 0.");

        if (options.TrafficAnomalyWindowSeconds <= 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:TrafficAnomalyWindowSeconds must be > 0.");

        if (options.InboxWindowHours < 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:InboxWindowHours must be >= 0.");

        foreach (var (qName, cap) in options.LocalQueues)
        {
            if (string.IsNullOrWhiteSpace(qName))
                errors.Add($"{AvtoBusConfiguration.SectionName}:LocalQueues key must not be empty.");
            if (cap <= 0)
                errors.Add($"{AvtoBusConfiguration.SectionName}:LocalQueues[{qName}] capacity must be > 0.");
        }

        if (options.Recoverability.ImmediateRetries < 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:Recoverability:ImmediateRetries must be >= 0.");

        if (options.Recoverability.DelayedRetries < 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:Recoverability:DelayedRetries must be >= 0.");

        if (options.Recoverability.DelayedBackoffBaseSeconds <= 0)
            errors.Add($"{AvtoBusConfiguration.SectionName}:Recoverability:DelayedBackoffBaseSeconds must be > 0.");

        if (options.Recoverability.DelayedBackoffMaxSeconds < options.Recoverability.DelayedBackoffBaseSeconds)
            errors.Add($"{AvtoBusConfiguration.SectionName}:Recoverability:DelayedBackoffMaxSeconds " +
                       "must be >= DelayedBackoffBaseSeconds.");

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }
}
