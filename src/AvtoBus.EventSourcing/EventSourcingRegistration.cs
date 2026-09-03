using System.Reflection;
using AvtoBus.Configuration;
using AvtoBus.EventSourcing;
using AvtoBus.EventSourcing.Projections;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus;

/// <summary>
/// Регистрация event sourcing: <c>bus.UseEventSourcing&lt;AppDbContext&gt;()</c> включает
/// стораж, репозиторий агрегатов и daemon проекций (идеи 251–260).
/// Пользователь подключает <c>modelBuilder.ConfigureEventSourcing()</c> к своему контексту.
/// </summary>
public static class EventSourcingRegistration
{
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2072", Justification =
        "Типы проекций регистрирует явно само приложение (options.Projections); DI требует публичный конструктор — задокументировано.")]
    public static BusConfigurator UseEventSourcing<TDb>(
        this BusConfigurator bus,
        Action<EventSourcingOptions>? configure = null,
        bool publishToBus = true)
        where TDb : DbContext
    {
        var options = new EventSourcingOptions();
        configure?.Invoke(options);

        bus.Services.AddSingleton(options);

        bus.Services.AddSingleton(sp => sp.GetRequiredService<EventSourcingOptions>().SnapshotPolicy);

        if (options.Encryption.Configs.Count > 0)
        {
            bus.Services.AddSingleton<ISubjectKeyRing>(options.KeyRing ?? new InMemorySubjectKeyRing());
            bus.Services.AddSingleton(sp => new SubjectDataProtection(
                sp.GetRequiredService<ISubjectKeyRing>(), options.Encryption));
            bus.Services.AddSingleton<IGdprReportService, GdprReportService>();
        }

        bus.Services.AddSingleton<IEventSerializer>(sp =>
        {
            var o = sp.GetRequiredService<EventSourcingOptions>();
            var serializer = new JsonEventSerializer(o.EventTypes);

            if (o.Encryption.Configs.Count > 0)
            {
                var protection = sp.GetRequiredService<SubjectDataProtection>();
                return new EncryptingEventSerializer(serializer, protection);
            }

            return serializer;
        });

        bus.Services.AddSingleton(sp =>
        {
            var o = sp.GetRequiredService<EventSourcingOptions>();
            return new UpcasterChain(sp.GetServices<IUpcaster>());
        });

        bus.Services.AddSingleton<IEventStore, EfCoreEventStore<TDb>>();

        bus.Services.AddSingleton<IAggregateRepository>(sp => new AggregateRepository(
            sp.GetRequiredService<IEventStore>(),
            sp.GetRequiredService<IEventSerializer>(),
            sp.GetRequiredService<UpcasterChain>(),
            sp.GetRequiredService<SnapshotPolicy>(),
            TimeProvider.System,
            publishToBus ? sp.GetService<IBus>() : null));

        foreach (var projectionType in options.Projections)
            bus.Services.AddSingleton(typeof(IProjection), projectionType);

        bus.Services.AddSingleton<IProjectionManager>(sp => new ProjectionManager(
            sp.GetRequiredService<IEventStore>(),
            sp.GetServices<IProjection>(),
            sp.GetRequiredService<IEventSerializer>(),
            sp.GetRequiredService<UpcasterChain>(),
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<ProjectionManager>>()));

        foreach (var subscription in options.StoreSubscriptions)
        {
            bus.Services.AddHostedService(sp => new StoreEventSubscription(
                sp.GetRequiredService<IEventStore>(),
                sp.GetRequiredService<IEventSerializer>(),
                sp.GetRequiredService<UpcasterChain>(),
                sp.GetRequiredService<IBus>(),
                subscription,
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<StoreEventSubscription>>()));
        }

        if (options.Projections.Count > 0)
        {
            bus.Services.AddSingleton(new ProjectionDaemonOptions());
            bus.Services.AddHostedService<ProjectionDaemon>();
        }

        return bus;
    }
}

public sealed class EventSourcingOptions
{
    public List<Type> EventTypes { get; } = new();

    public List<Type> Projections { get; } = new();

    public List<StoreSubscriptionOptions> StoreSubscriptions { get; } = new();

    public SnapshotPolicy SnapshotPolicy { get; } = new();

    public SubjectEncryptionConfigurator Encryption { get; } = new();

    /// <summary>Кольцо ключей субъектов; по умолчанию in-memory.</summary>
    public ISubjectKeyRing? KeyRing { get; set; }

    public EventSourcingOptions Encrypt(Action<SubjectEncryptionConfigurator> configure)
    {
        configure(Encryption);
        return this;
    }

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Сканирование сборки на события несовместимо с trimming. Регистрируйте типы явно через EventTypes.")]
    public EventSourcingOptions AddEventsFromAssembly(Assembly assembly)
    {
        foreach (var t in assembly.GetTypes())
        {
            if (t is { IsClass: true, IsAbstract: false }
                && (t.Name.EndsWith("Event", StringComparison.Ordinal)
                    || t.GetCustomAttributes(typeof(MessageAliasAttribute), false).Length > 0))
            {
                EventTypes.Add(t);
            }
        }

        return this;
    }

    public EventSourcingOptions AddEvents(params Type[] eventTypes)
    {
        EventTypes.AddRange(eventTypes);
        return this;
    }

    public EventSourcingOptions AddProjection<TProjection>() where TProjection : IProjection
    {
        Projections.Add(typeof(TProjection));
        return this;
    }

    /// <summary>Публикация событий стора в шину (идея 269). <paramref name="streamType"/> — фильтр по типу стрима.</summary>
    public EventSourcingOptions PublishStoreEvents(string? streamType = null, string? name = null)
    {
        StoreSubscriptions.Add(new StoreSubscriptionOptions
        {
            Name = name ?? streamType ?? "store-all",
            StreamType = streamType,
        });
        return this;
    }

    public EventSourcingOptions SnapshotEvery<TAggregate>(int events) where TAggregate : Aggregate
    {
        SnapshotPolicy.For<TAggregate>(events);
        return this;
    }
}
