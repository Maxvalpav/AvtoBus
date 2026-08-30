# AvtoBus

Современный EDA-фреймворк для ASP.NET Core. Простой как Wolverine, надёжный как NServiceBus.
Даёшь сообщение — **садись и езжай** (🏁).

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-purple)](global.json)

## Что это

- **Конверт с корреляцией**: `CorrelationId`, `CausationId`, W3C `traceparent` в каждом сообщении —
  один бизнес-поток — один идентификатор (идеи 12, 195).
- **Пайплайн middleware** как в ASP.NET Core: дедуп, circuit breaker, recoverability
  (ретраи → DLQ), батчи, саги — всё это просто `IBusMiddleware`.
- **Транзакционный outbox** на EF Core: каскады отправляются только после коммита (идея 6).
- **Саги** с durable выполненением и повторным воспроизведением.
- **Source generator** вместо reflection для хендлеров (идея 110).
- **Наблюдаемость из коробки**: OTel-трейсы и метрики по конвенциям messaging, `AvtoBus-Diagnostics`
  EventSource, канарейка, детектор аномалий трафика, чёрный список на лету, аудит «кто послал».
- **Безопасность конвертов**: подпись HMAC-SHA256 и шифрование AES-256-GCM с ротацией ключей,
  авторизация хендлеров `[BusAuthorize]`, проброс пользователя через подписанный заголовок,
  маскирование PII (идеи 451–456, 459).
- **Тест-харнесс**: вся шина в памяти за одну строку (идея 316).

## Быстрый старт

```bash
dotnet add package AvtoBus
```

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
});

app.MapPost("/orders", async (PlaceOrder cmd, IBus bus) =>
{
    await bus.SendAsync(cmd);   // команда уйдёт в очередь, консьюмер обработает
    return Results.Accepted();
});

await app.RunAsync();

// Консьюмер — просто метод по конвенции:
public static class OrderHandlers
{
    public static Task Handle(PlaceOrder cmd, ConsumeContext ctx)
        => ctx.PublishAsync(new OrderPlaced(cmd.OrderId, cmd.Total));
}
```

Документация — `docs/22-getting-started.md`.

## Проекты

| Пакет | Назначение |
|---|---|
| `AvtoBus` | метапакет: Core + InMemory + JSON — единая точка входа |
| `AvtoBus.Templates` | шаблоны проектов: `dotnet new avtobus-worker`, `dotnet new avtobus-webapi` (идея 401) |
| `AvtoBus.Core` | ядро: конверт, пайплайн, recoverability, метрики |
| `AvtoBus.InMemory` | in-memory транспорт |
| `AvtoBus.RabbitMq` | RabbitMQ-транспорт: quorum queues, stream-топики (retention), publisher confirms, DLQ |
| `AvtoBus.Outbox.EfCore` | транзакционный outbox на EF Core |
| `AvtoBus.Sagas` | саги и durable execution |
| `AvtoBus.Scheduling` | cron, отложенные сообщения, leader election |
| `AvtoBus.Kafka` | Kafka-транспорт: exactly-once, партиции по ключу, back-pressure |
| `AvtoBus.Nats` | NATS/JetStream-транспорт: durable push-consumers, queue groups, KV |
| `AvtoBus.Redis` | Redis Streams-транспорт: consumer groups, XAUTOCLAIM |
| `AvtoBus.Sql` | SQL-транспорт: PostgreSQL таблица-очередь, SKIP LOCKED + LISTEN/NOTIFY |
| `AvtoBus.AzureServiceBus` | Azure Service Bus-транспорт: сессии, scheduled enqueue, lock renew |
| `AvtoBus.EventSourcing` | Event Store, агрегаты, проекции, snapshots, crypto-shredding + GDPR, blue/green реплей, мини-DSL стрим-процессинга |
| `AvtoBus.Security` | подписи, шифрование, авторизация, PII |
| `AvtoBus.Multitenancy` | мультитенантность: уровни A/B/C, data-residency guard, per-tenant квоты |
| `AvtoBus.Generators` | source generator |
| `AvtoBus.Analyzers` | Roslyn-анализаторы и code-fixes для контрактов, маршрутизации и нейминга (AVB001-060) |
| `AvtoBus.AsyncApi` | генератор AsyncAPI 3.0 спецификации из модели шины (контракты, маршруты, схемы) |
| `AvtoBus.EventCatalog` | словарь доменных событий: статический HTML-сайт и JSON-каталог из модели шины (идеи 137, 138) |
| `AvtoBus.Cli` | dotnet tool `avtobus`: doctor, contracts, es explain, config, dlq, completion |
| `AvtoBus.Aspire` | интеграция с .NET Aspire: `AddAvtoBusRabbit()`, `WithAvtoBus()` (RabbitMQ + PostgreSQL ресурсы в AppHost, идея 419) |
| `AvtoBus.Testing` | тест-харнесс |

## Документация

- `docs/01-architecture.md` — архитектура и видение
- `docs/02-competitors.md` — анализ MassTransit/NServiceBus/Rebus и ещё 20+
- `docs/04..13` — 500 идей (404, 464, ...)
- `docs/14..18` — реализации (core, outbox, sourcegen, sagas, transports)
- `docs/code/11..18` — код: маркеры, структура, тесты, CI/CD, scheduling, event sourcing, security, финальные пробелы
- `docs/22-getting-started.md` — как начать

## Разработка

```bash
dotnet build
dotnet test
```

Требуется .NET 10 SDK. Как вносить изменения — `CONTRIBUTING.md`.

## Лицензия

MIT. Подробности — `LICENSE`.