using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace AvtoBus;

/// <summary>
/// Регистрация AvtoBus в OpenTelemetry (B6). Без этого трейсы и метрики шины
/// не будут экспортироваться: источники <see cref="Observability.BusTelemetry.ActivitySourceName"/>
/// и <see cref="Observability.BusTelemetry.MeterName"/> не подписаны на провайдеры.
/// </summary>
public static class AvtoBusOpenTelemetryExtensions
{
    /// <summary>Добавить трейсинг AvtoBus: подписывает провайдер на ActivitySource шины.</summary>
    public static TracerProviderBuilder AddAvtoBusInstrumentation(this TracerProviderBuilder builder)
        => builder.AddSource(Observability.BusTelemetry.ActivitySourceName);

    /// <summary>Добавить метрики AvtoBus: подписывает провайдер на Meter шины.</summary>
    public static MeterProviderBuilder AddAvtoBusInstrumentation(this MeterProviderBuilder builder)
        => builder.AddMeter(Observability.BusTelemetry.MeterName);
}
