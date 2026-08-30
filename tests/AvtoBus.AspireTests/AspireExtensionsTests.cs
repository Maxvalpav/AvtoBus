using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using AvtoBus.Aspire;

namespace AvtoBus.AspireTests;

/// <summary>
/// Тесты построения модели ресурсов Aspire (без запуска контейнеров).
/// Проверяем, что extension-методы корректно конфигурируют ресурсы.
/// </summary>
public class AspireExtensionsTests
{
    private static IDistributedApplicationBuilder CreateBuilder(string[]? args = null)
        => DistributedApplication.CreateBuilder(args ?? []);

    [Fact]
    public void AddAvtoBusRabbit_creates_named_rabbit_resource()
    {
        var builder = CreateBuilder();

        var rabbit = builder.AddAvtoBusRabbit("orders-rabbit");

        Assert.IsType<RabbitMQServerResource>(rabbit.Resource);
        Assert.Equal("orders-rabbit", rabbit.Resource.Name);
    }

    [Fact]
    public void AddAvtoBusRabbit_uses_default_name()
    {
        var builder = CreateBuilder();

        var rabbit = builder.AddAvtoBusRabbit();

        Assert.Equal("avtobus-rabbit", rabbit.Resource.Name);
    }

    [Fact]
    public void AddPostgres_database_resource_is_created()
    {
        var builder = CreateBuilder();

        var db = builder.AddPostgres("avtobus-pg").AddDatabase("orders");

        Assert.Equal("orders", db.Resource.Name);
    }
}
