using AvtoBus.Configuration;
using AvtoBus.Outbox.EfCore;
using AvtoBus.Scheduling;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus;

/// <summary>
/// Регистрация шедулинга: <c>bus.UseScheduling&lt;AppDbContext&gt;()</c> включает durable
/// отложенные сообщения и cron (идеи 223, 226). Пользователь подключает
/// <c>modelBuilder.ConfigureScheduling().ConfigureLeaderLease()</c> к своему контексту.
/// </summary>
public static class SchedulingRegistration
{
    public static BusConfigurator UseScheduling<TDb>(
        this BusConfigurator bus,
        Action<SchedulerOptions>? configure = null,
        bool useEfCore = true)
        where TDb : DbContext
    {
        var options = new SchedulerOptions();
        configure?.Invoke(options);

        bus.Services.AddSingleton(options);

        if (!bus.Services.Any(d => d.ServiceType == typeof(IEnvelopeSerializer)))
            bus.Services.AddSingleton<IEnvelopeSerializer, JsonEnvelopeSerializer>();

        bus.Services.AddSingleton<IEnvelopeFactory, EnvelopeCodecFactory>();

        if (useEfCore)
        {
            bus.Services.AddSingleton<IScheduleStore, EfCoreScheduleStore<TDb>>();
            bus.Services.AddSingleton<ILeaderElection, EfCoreLeaderElection<TDb>>();
        }
        else
        {
            bus.Services.AddSingleton<IScheduleStore, InMemoryScheduleStore>();
            bus.Services.AddSingleton<ILeaderElection, InMemoryLeaderElection>();
        }

        bus.Services.AddSingleton<ICronRegistry, CronRegistry>();
        bus.Services.AddSingleton<IScheduler, DurableScheduler>();
        bus.Services.AddHostedService<CronBootstrapper>();
        bus.Services.AddHostedService<SchedulerService>();
        return bus;
    }
}
