using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace AvtoBus.Aspire;

/// <summary>
/// .NET Aspire integration: добавляем AvtoBus ресурсы в AppHost.
/// </summary>
public static class AspireExtensions
{
    /// <summary>
    /// Добавить RabbitMQ (persistent) с management-плагином.
    /// </summary>
    public static IResourceBuilder<RabbitMQServerResource> AddAvtoBusRabbit(
        this IDistributedApplicationBuilder builder,
        string name = "avtobus-rabbit")
    {
        return builder.AddRabbitMQ(name)
            .WithManagementPlugin()
            .WithLifetime(ContainerLifetime.Persistent);
    }

    /// <summary>
    /// Подключить проект к AvtoBus RabbitMQ (+ необязательно PostgreSQL).
    /// </summary>
    public static IResourceBuilder<ProjectResource> WithAvtoBus(
        this IResourceBuilder<ProjectResource> project,
        IResourceBuilder<RabbitMQServerResource> rabbit,
        IResourceBuilder<PostgresServerResource>? postgres = null)
    {
        var result = project.WithReference(rabbit);

        if (postgres is not null)
            result = result.WithReference(postgres);

        return result.WithEnvironment("AVTOBUS_TRANSPORT", "rabbitmq");
    }

    /// <summary>
    /// Подключить проект к AvtoBus PostgreSQL (Event Store / outbox).
    /// </summary>
    public static IResourceBuilder<ProjectResource> WithAvtoBusPostgres(
        this IResourceBuilder<ProjectResource> project,
        IResourceBuilder<PostgresDatabaseResource> database)
    {
        return project
            .WithReference(database)
            .WithEnvironment("AVTOBUS_STORAGE", "postgres");
    }
}
