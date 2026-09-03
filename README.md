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
  <a href="https://github.com/Maxvalpav/AvtoBus/actions/workflows/ci.yml"><img src="https://github.com/Maxvalpav/AvtoBus/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/Maxvalpav/AvtoBus"><img src="https://codecov.io/gh/Maxvalpav/AvtoBus/branch/main/graph/badge.svg" alt="Coverage"></a>
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
    bus.UseProductionDefaults(); // ретраи + inbox-дедуп + circuit breaker в одну строку
});

// С базой и подписью — тот же один вызов:
// bus.UseProductionDefaults<AppDbContext>(o => o.MasterSecret = "shared-secret");

var app = builder.Build();

app.MapPost("/orders", async (PlaceOrder cmd, IBus bus) =>
{
    await bus.SendAsync(cmd);   // команда уйдёт в очередь, консьюмер обработает
    return Results.Accepted();
});

await app.RunAsync();

// Консьюмер — просто метод по конвенции (канонический стиль):
public static class OrderHandlers
{
    public static Task Handle(PlaceOrder cmd, ConsumeContext ctx)
        => ctx.PublishAsync(new OrderPlaced(cmd.OrderId, cmd.Total));
}
```

## 📦 Проекты

| Пакет | Назначение | Доставка |
|---|---|---|
| `AvtoBus` | метапакет: Core + InMemory + JSON — единая точка входа | — |
| `AvtoBus.Templates` | шаблоны проектов: `dotnet new avtobus-worker`, `dotnet new avtobus-webapi` | — |
| `AvtoBus.Core` | ядро: конверт, пайплайн, recoverability, метрики | — |
| `AvtoBus.InMemory` | in-memory транспорт | at-least-once в процессе |
| `AvtoBus.RabbitMq` | RabbitMQ-транспорт: quorum queues, stream-топики, publisher confirms, DLQ | at-least-once, порядок в очереди |
| `AvtoBus.Outbox.EfCore` | транзакционный outbox на EF Core | атомарность с бизнес-транзакцией |
| `AvtoBus.Sagas` | саги и durable execution | — |
| `AvtoBus.Scheduling` | cron, отложенные сообщения, leader election | — |
| `AvtoBus.Kafka` | Kafka-транспорт: партиции по ключу, back-pressure | at-least-once; exactly-once опционально (транзакции) |
| `AvtoBus.Nats` | NATS/JetStream-транспорт: durable push-consumers, queue groups, KV | at-least-once |
| `AvtoBus.Redis` | Redis Streams-транспорт: consumer groups, XAUTOCLAIM | at-least-once |
| `AvtoBus.Sql` | SQL-транспорт: PostgreSQL таблица-очередь, SKIP LOCKED + LISTEN/NOTIFY | at-least-once |
| `AvtoBus.AzureServiceBus` | Azure Service Bus-транспорт: сессии, scheduled enqueue, lock renew | at-least-once (PeekLock) |
| `AvtoBus.EventSourcing` | Event Store, агрегаты, проекции, snapshots, crypto-shredding + GDPR, blue/green реплей | — |
| `AvtoBus.Security` | подписи, шифрование, авторизация, PII | — |
| `AvtoBus.Multitenancy` | мультитенантность: уровни A/B/C, data-residency guard, per-tenant квоты | — |
| `AvtoBus.Generators` | source generator | — |
| `AvtoBus.Analyzers` | Roslyn-анализаторы и code-fixes (AVB001-060) | — |
| `AvtoBus.AsyncApi` | генератор AsyncAPI 3.0 из модели шины | — |
| `AvtoBus.EventCatalog` | словарь доменных событий: статический HTML-сайт и JSON-каталог | — |
| `AvtoBus.Cli` | dotnet tool `avtobus`: doctor, contracts, es explain, config, dlq, completion | — |
| `AvtoBus.Aspire` | интеграция с .NET Aspire | — |
| `AvtoBus.Dashboard` | встраиваемый дашборд: обзор, DLQ (за auth-политикой, опасные действия в проде запрещены) | — |
| `AvtoBus.Bridge` | мост между транспортами (например, Kafka ↔ RabbitMQ) | at-least-once |
| `AvtoBus.Abstractions` | базовые абстракции шины | — |
| `AvtoBus.Streams` | стрим-процессинг: окна, join'ы, state stores | — |
| `AvtoBus.Workflow` | durable workflow: таймеры, активности, сигналы | — |
| `AvtoBus.Durability.PostgreSql` | durability-примитивы на PostgreSQL | — |
| `AvtoBus.SchemaRegistry` | реестр схем контрактов | — |
| `AvtoBus.Serialization.MessagePack` | MessagePack-сериализатор | — |
| `AvtoBus.Serialization.Protobuf` | Protobuf-сериализатор | — |
| `AvtoBus.Testing` | тест-харнесс | — |

## 🚚 Примеры

| Пример | Что показывает |
|---|---|
| `samples/AvtoBus.QuickStart` | минимум: шина + RabbitMQ + outbox за 5 минут |
| `samples/AvtoBus.TwoServices` | два сервиса (Orders/Inventory) через брокер |
| `samples/AvtoBus.Logistics` | 30 микросервисов на in-memory транспорте |
| `samples/AvtoBus.AotSample` | Native AOT без рефлексии |

## 🛠 Разработка

```bash
dotnet build
dotnet test
```

Требуется .NET 10 SDK.

## 📄 Лицензия

MIT. Подробности — [`LICENSE`](LICENSE).
