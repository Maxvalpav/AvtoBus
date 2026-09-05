# Getting Started

Минимальный путь от нуля до первого сообщения — модульный монолит без брокера.

## 1. Установка

```bash
dotnet add package AvtoBus
```

Требуется .NET 10 SDK (точная версия — в `global.json` репозитория).

## 2. Первое сообщение

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.UseProductionDefaults(); // ретраи + inbox-дедуп + circuit breaker в одну строку
});

var app = builder.Build();

app.MapPost("/orders", async (PlaceOrder cmd, IBus bus) =>
{
    await bus.SendAsync(cmd);
    return Results.Accepted();
});

await app.RunAsync();

public static class OrderHandlers
{
    public static Task Handle(PlaceOrder cmd, ConsumeContext ctx)
        => ctx.PublishAsync(new OrderPlaced(cmd.OrderId, cmd.Total));
}
```

Консьюмер — статический метод по конвенции `Handle`. Каскад `PublishAsync`
уйдёт дальше по шине в той же корреляции (`CorrelationId`/`CausationId` наследуются).

## 3. С базой и подписью

```csharp
builder.Services.AddAvtoBus(bus =>
{
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    // Секрет — из конфигурации, никогда литералом в коде:
    bus.UseProductionDefaults<AppDbContext>(o =>
        o.MasterSecret = builder.Configuration["AvtoBus:MasterSecret"]!);
});
```

Полный пресет добавляет транзакционный outbox и подпись конвертов.
Подробности: [outbox](outbox.md), [security](security.md).

## 4. Куда дальше

- [Какой транспорт выбрать](decision-guide.md) — когда InMemory перестаёт хватать.
- [Гарантии доставки](guarantees.md) — что шина обещает, а что должны вы (идемпотентность!).
- [Наблюдаемость](observability.md) — метрики и трейсы из коробки.
- `samples/AvtoBus.QuickStart` — рабочий пример с RabbitMQ + outbox.
