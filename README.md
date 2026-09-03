<p align="center">
  <img src="assets/avtobus-hero.svg" width="720" alt="AvtoBus — садись и езжай">
</p>

<h1 align="center">AvtoBus</h1>

<p align="center">
  <b>Современный EDA-фреймворк для ASP.NET Core.</b><br>
  Конверты, саги, outbox и наблюдаемость из коробки.<br>
  Даёшь сообщение — <b>садись и езжай</b> 🏁
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <a href="global.json"><img src="https://img.shields.io/badge/.NET-10.0-purple" alt=".NET 10"></a>
  <img src="https://img.shields.io/badge/transports-7-green" alt="7 transports">
  <img src="https://img.shields.io/badge/tests-370+-brightgreen" alt="tests">
</p>

---

## ✨ Что это

| | Возможность |
|---|---|
| 💌 | **Конверт с корреляцией** — `CorrelationId`, `CausationId`, W3C `traceparent` в каждом сообщении: один бизнес-поток — один идентификатор |
| 🛤 | **Пайплайн как в ASP.NET Core** — дедуп, circuit breaker, recoverability (ретраи → DLQ), батчи, саги: всё это просто `IBusMiddleware` |
| 📦 | **Транзакционный outbox** на EF Core — каскады отправляются только после коммита |
| 🔄 | **Саги** с durable-выполнением и повторным воспроизведением |
| ⚡ | **Source generator** вместо reflection для хендлеров |
| 🔭 | **Наблюдаемость из коробки** — OTel-трейсы и метрики, `AvtoBus-Diagnostics` EventSource, канарейка, детектор аномалий, чёрный список на лету, аудит «кто послал» |
| 🔐 | **Безопасность конвертов** — HMAC-SHA256 (схема v2 покрывает маршрутизацию) и AES-256-GCM с ротацией ключей, `[BusAuthorize]`, маскирование PII |
| 🧪 | **Тест-харнесс** — вся шина в памяти за одну строку |

## 🚀 Быстрый старт

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

Быстрый старт — в секции ниже: установка пакета, регистрация шины, первый консьюмер.

## 📦 Проекты

| Пакет | Назначение |
|---|---|
| `AvtoBus` | метапакет: Core + InMemory + JSON — единая точка входа |
| `AvtoBus.Templates` | шаблоны проектов: `dotnet new avtobus-worker`, `dotnet new avtobus-webapi` |
| `AvtoBus.Core` | ядро: конверт, пайплайн, recoverability, метрики |
| `AvtoBus.InMemory` | in-memory транспорт |
| `AvtoBus.RabbitMq` | RabbitMQ-транспорт: quorum queues, stream-топики, publisher confirms, DLQ |
| `AvtoBus.Outbox.EfCore` | транзакционный outbox на EF Core |
| `AvtoBus.Sagas` | саги и durable execution |
| `AvtoBus.Scheduling` | cron, отложенные сообщения, leader election |
| `AvtoBus.Kafka` | Kafka-транспорт: exactly-once, партиции по ключу, back-pressure |
| `AvtoBus.Nats` | NATS/JetStream-транспорт: durable push-consumers, queue groups, KV |
| `AvtoBus.Redis` | Redis Streams-транспорт: consumer groups, XAUTOCLAIM |
| `AvtoBus.Sql` | SQL-транспорт: PostgreSQL таблица-очередь, SKIP LOCKED + LISTEN/NOTIFY |
| `AvtoBus.AzureServiceBus` | Azure Service Bus-транспорт: сессии, scheduled enqueue, lock renew |
| `AvtoBus.EventSourcing` | Event Store, агрегаты, проекции, snapshots, crypto-shredding + GDPR, blue/green реплей |
| `AvtoBus.Security` | подписи, шифрование, авторизация, PII |
| `AvtoBus.Multitenancy` | мультитенантность: уровни A/B/C, data-residency guard, per-tenant квоты |
| `AvtoBus.Generators` | source generator |
| `AvtoBus.Analyzers` | Roslyn-анализаторы и code-fixes (AVB001-060) |
| `AvtoBus.AsyncApi` | генератор AsyncAPI 3.0 из модели шины |
| `AvtoBus.EventCatalog` | словарь доменных событий: статический HTML-сайт и JSON-каталог |
| `AvtoBus.Cli` | dotnet tool `avtobus`: doctor, contracts, es explain, config, dlq, completion |
| `AvtoBus.Aspire` | интеграция с .NET Aspire |
| `AvtoBus.Testing` | тест-харнесс |

## 🛠 Разработка

```bash
dotnet build
dotnet test
```

Требуется .NET 10 SDK.

## 📄 Лицензия

MIT. Подробности — [`LICENSE`](LICENSE).
