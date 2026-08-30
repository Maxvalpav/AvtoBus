# 🚀 Getting Started: от `dotnet new` до первого события за 5 минут

> **Target tutorial / not runnable yet.** Команды и API описывают желаемый опыт после создания NuGet-пакетов и sample-проектов.

## 1. Предварительные требования

- .NET SDK 10+ (поддерживается и 11)
- Редактор с Roslyn (Rider 2025+ / VS 2022 / VS Code + C# extension)
- Опционально: Docker (для RabbitMQ/Postgres)

## 2. Создаём новый проект

```bash
# Устанавливаем шаблоны AvtoBus (один раз)
dotnet new install AvtoBus.Templates

# Создаём worker + RabbitMQ + Outbox на PostgreSQL
dotnet new avtobus-worker -n OrderService --transport rabbit --outbox postgres

cd OrderService
```

Или с нуля:

```bash
dotnet new web -n OrderService
cd OrderService
dotnet add package AvtoBus
dotnet add package AvtoBus.RabbitMq
dotnet add package AvtoBus.Outbox.EfCore
dotnet add package AvtoBus.Generators
dotnet add package Npgsql.EntityFrameworkCore.PostgreSQL
```

## 3. Определим контракты

```csharp
// Contracts/PlaceOrder.cs
namespace OrderService.Contracts;

// Команда — ожидает ровно один обработчик
public sealed record PlaceOrder(
    Guid OrderId,
    string CustomerId,
    OrderItem[] Items
) : ICommand;

public sealed record OrderItem(string Sku, int Qty, decimal Price);

// Событие — может обрабатываться многими подписчиками
public sealed record OrderPlaced(
    Guid OrderId,
    string CustomerId,
    decimal Total
) : IEvent;
```

## 4. Хендлер

```csharp
// Handlers/OrderHandlers.cs
using OrderService.Contracts;
using Microsoft.EntityFrameworkCore;

namespace OrderService.Handlers;

// Статический класс + статический метод — ноль церемоний
public static class OrderHandlers
{
    // Зависимости (DbContext) — инжектятся по параметру
    // Возврат OrderPlaced — автоматически публикуется каскадно через Outbox
    public static async Task<OrderPlaced> Handle(
        PlaceOrder cmd,
        OrderDbContext db,
        CancellationToken ct)
    {
        var order = new Order(cmd.OrderId, cmd.CustomerId, cmd.Items);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct); // ← order + outbox-запись в одной транзакции

        return new OrderPlaced(cmd.OrderId, cmd.CustomerId, order.Total);
    }
}
```

## 5. Подключаем AvtoBus в Program.cs

```csharp
// Program.cs
using AvtoBus;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<OrderDbContext>(opt =>
    opt.UseNpgsql(builder.Configuration.GetConnectionString("Db")));

builder.Services.AddAvtoBus(bus =>
{
    // 1) Транспорт
    bus.UseRabbitMq(bus.Configuration.GetConnectionString("Rabbit")!);

    // 2) Надёжность
    bus.UseOutbox<OrderDbContext>(o =>
    {
        o.BatchSize = 200;
        o.CleanupAfter(TimeSpan.FromDays(7));
    });
    bus.UseInboxDeduplication(window: TimeSpan.FromHours(24));

    // 3) Recoverability как у NServiceBus
    bus.Recoverability(r =>
    {
        r.ImmediateRetries(3);
        r.DelayedRetries(5, Backoff.DecorrelatedJitter(TimeSpan.FromSeconds(5)));
        r.MapException<ValidationException>(FailureAction.Discard);
    });

    // 4) Хендлеры из этой сборки (Source Generator найдёт их автоматически)
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);

    // 5) Observability — OTel включено по умолчанию
    bus.Pipeline(p => p.UseFluentValidation());
});

// Дашборд шины на /bus
builder.Services.AddAvtoBusDashboard();

var app = builder.Build();

app.MapGet("/", () => "OrderService running");
app.MapAvtoBusDashboard("/bus");

// Инициализируем БД (outbox-таблицы + EF migrations)
await using var scope = app.Services.CreateAsyncScope();
await scope.ServiceProvider.GetRequiredService<OrderDbContext>().Database.MigrateAsync();

app.Run();
```

## 6. Connection strings (`appsettings.json`)

```json
{
  "ConnectionStrings": {
    "Db": "Host=localhost;Database=orders;Username=app;Password=app",
    "Rabbit": "amqp://guest:guest@localhost:5672"
  },
  "AllowedHosts": "*"
}
```

## 7. Поднимаем инфраструктуру (docker-compose)

Сохрани как `docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment: { POSTGRES_USER: app, POSTGRES_PASSWORD: app, POSTGRES_DB: orders }
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]

  rabbitmq:
    image: rabbitmq:4-management-alpine
    ports: ["5672:5672", "15672:15672"]

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports: ["4317:4317", "16686:16686"]

volumes: { pgdata: {} }
```

```bash
docker compose up -d
```

## 8. Запускаем

```bash
dotnet run
```

Открываем:
- API: `http://localhost:5000`
- Дашборд AvtoBus: `http://localhost:5000/bus`
- RabbitMQ Management: `http://localhost:15672` (guest/guest)
- Jaeger трейсы: `http://localhost:16686`

## 9. Отправляем первую команду

```bash
curl -X POST http://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "8e7d5f9a-4f77-4b89-a2bc-1a2b3c4d5e6f",
    "customerId": "cust-42",
    "items": [{"sku": "sku-001", "qty": 2, "price": 1500}]
  }'
```

В ответ придёт `OrderPlaced`. В Jaeger увидишь полный трейс:
`POST /api/orders → PlaceOrder → Outbox enqueue → Relay → RabbitMQ → Handler → OrderPlaced`.

## 10. Добавим подписчика на событие (ShippingService)

Аналогично создаём второй сервис и добавляем хендлер:

```csharp
// ShippingService/Handlers/OrderPlacedHandler.cs
public static class ShippingHandlers
{
    // Когда опубликовано OrderPlaced — создаём отгрузку и шлём email
    public static async Task Handle(
        OrderPlaced evt,
        IShipmentService shipments,
        IEmailService email,
        CancellationToken ct)
    {
        var shipment = await shipments.Create(evt.OrderId, ct);
        await email.SendConfirmation(evt.CustomerId, evt.OrderId, shipment.Id, ct);
    }
}
```

Никакой конфигурации, кроме адреса RabbitMQ — событие автоматически доходит до подписчика.

## 11. Тестируем за 5 строк

```csharp
// Tests/OrderHandlerTests.cs
public class OrderHandlerTests
{
    [Fact]
    public async Task PlaceOrder_publishes_OrderPlaced()
    {
        await using var h = await AvtoBusTestHarness.CreateAsync(services =>
        {
            // реальная реализация IOrderRepository
            services.AddScoped<OrderDbContext>(_ => BuildInMemoryDb());
        });

        var cmd = new PlaceOrder(Guid.NewGuid(), "cust-1", []);
        await h.Bus.Send(cmd);

        var consumed = await h.WaitForConsumed<PlaceOrder>(TimeSpan.FromSeconds(5));
        consumed.Should().ContainSingle();

        var published = h.Published<OrderPlaced>().Single();
        published.CustomerId.Should().Be("cust-1");
    }
}
```

## 12. Команды CLI на каждый день

```bash
dotnet avtobus doctor                      # проверить инфраструктуру
dotnet avtobus topology apply              # создать exchange/queues/bindings
dotnet avtobus dlq list orders.error       # посмотреть мёртвые сообщения
dotnet avtobus dlq replay orders.error --filter 'type=PlaceOrder'
dotnet avtobus saga list OrderSaga         # активные саги
dotnet avtobus projections rebuild Invoices
dotnet avtobus bench --transport rabbit    # прогнать бенч
```

## 🎉 Что дальше?

- Раздел [04..13](./README.md) — 500 идей по темам
- [14-implementation-core.md](./14-implementation-core.md) — как устроено ядро изнутри
- [19-roadmap.md](./19-roadmap.md) — план выхода на v1.0
- [26-example-end2end.md](./26-example-end2end.md) — полный пример e-commerce с сагой оплаты

### Полезные сниппеты для 90% задач

```csharp
// Отложенное сообщение
await bus.Schedule(new RemindOrder(orderId), at: DateTimeOffset.UtcNow.AddDays(1));

// Request/response
var quote = await bus.Request<GetQuote, QuoteResult>(new GetQuote("MSFT"), 5.Seconds());

// Батчевый хендлер
public static Task Handle(IMessageBatch<PriceTick> batch) => BulkInsert(batch.Messages);

// Сагу см. в 17-implementation-sagas.md
```
