# 🏗 AvtoBus — Архитектура

> **Статус: Specification.** Описывает целевые границы модулей. При расхождении с `docs/code/*` этот файл и `03-core-api.md` имеют приоритет — см. `32-documentation-policy.md`.

## 1. Видение

AvtoBus — это EDA-фреймворк, который берёт лучшее из мировых решений:

- **Wolverine (C#)** → handler-ы как чистые методы + codegen вместо рефлексии
- **MassTransit (C#)** → богатая экосистема транспортов, саги, тест-харнесс
- **CAP (C#)** → простейший Outbox поверх локальной БД
- **NServiceBus (C#)** → recoverability-политики, message-driven state machines
- **Axon (Java)** → CQRS + Event Sourcing как единая модель
- **Watermill (Go)** → композиция через middleware-роутер, минимализм
- **Broadway (Elixir)** → back-pressure, батчинг, конвейерные стадии
- **Temporal (Go/Java)** → durable execution для саг
- **NATS JetStream** → лёгкость, скорость, KV/Object store поверх стрима
- **FastStream (Python)** → декларативность и автодокументация AsyncAPI

## 2. Слои архитектуры

```
┌─────────────────────────────────────────────────────────┐
│  Application Layer:  Handlers / Sagas / Projections     │
├─────────────────────────────────────────────────────────┤
│  AvtoBus.Core:  IBus, Envelope, Pipeline (middleware)   │
├───────────────┬───────────────┬─────────────────────────┤
│  Reliability  │   Scheduling  │   EventSourcing         │
│  Outbox/Inbox │   Cron/Delay  │   Store/Projections     │
├───────────────┴───────────────┴─────────────────────────┤
│  Serialization: JSON / MessagePack / Protobuf / Avro    │
├─────────────────────────────────────────────────────────┤
│  Transports: RabbitMQ │ Kafka │ ASB │ NATS │ Redis │ Mem│
└─────────────────────────────────────────────────────────┘
```

## 3. NuGet-пакеты (модульность)

| Пакет | Назначение |
|-------|-----------|
| `AvtoBus` | Метапакет: Core + InMemory + JSON |
| `AvtoBus.Core` | Абстракции, пайплайн, envelope |
| `AvtoBus.RabbitMq` | Транспорт RabbitMQ (streams, quorum queues) |
| `AvtoBus.Kafka` | Транспорт Kafka (+ exactly-once transactions) |
| `AvtoBus.AzureServiceBus` | Azure Service Bus (sessions, topics) |
| `AvtoBus.Nats` | NATS Core + JetStream |
| `AvtoBus.Redis` | Redis Streams |
| `AvtoBus.Sql` | SQL-транспорт а-ля SQL Server Transport NServiceBus |
| `AvtoBus.Outbox.EfCore` | Transactional Outbox/Inbox для EF Core |
| `AvtoBus.Outbox.Dapper` | Outbox для Dapper/ADO.NET |
| `AvtoBus.Sagas` | Саги и стейт-машины |
| `AvtoBus.Scheduling` | Отложенные и cron-сообщения |
| `AvtoBus.EventSourcing` | Event Store + проекции (как Marten) |
| `AvtoBus.Generators` | Source Generators (роутинг, сериализация) |
| `AvtoBus.Testing` | Тест-харнесс |
| `AvtoBus.Dashboard` | Web UI мониторинга |
| `AvtoBus.Cli` | dotnet tool `avtobus` |

## 4. Ключевые сущности ядра

### 4.1 Envelope — конверт сообщения

```csharp
public sealed record Envelope
{
    public required Guid MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public Guid? CausationId { get; init; }
    public required string MessageType { get; init; }     // "orders.order-placed.v1"
    public required ReadOnlyMemory<byte> Body { get; init; }
    public string ContentType { get; init; } = "application/json";
    public DateTimeOffset SentAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? DeliverAt { get; init; }        // отложенная доставка
    public TimeSpan? TimeToLive { get; init; }
    public string? PartitionKey { get; init; }             // упорядочивание
    public string? TenantId { get; init; }                 // мультитенантность
    public string? ReplyTo { get; init; }                  // request/response
    public int DeliveryAttempt { get; init; }
    public string? TraceParent { get; init; }              // W3C Trace Context
    public IReadOnlyDictionary<string, string> Headers { get; init; }
        = FrozenDictionary<string, string>.Empty;
}
```

### 4.2 Пайплайн обработки (middleware, как в ASP.NET Core)

```
Receive → Deserialize → Telemetry → TenantResolve → Inbox(dedup)
       → Retry → CircuitBreaker → Validation → Auth → UnitOfWork
       → Handler → Outbox(publish cascades) → Ack
```

Каждый шаг — `IBusMiddleware`, порядок настраивается, шаги можно заменять.

### 4.3 Топология по умолчанию (convention over configuration)

- Команда `PlaceOrder` → очередь `place-order` (один владелец)
- Событие `OrderPlaced` → exchange/topic `orders.order-placed` (fan-out подписчикам)
- Ошибки → `{queue}.error`, ретраи → `{queue}.retry.{n}` c TTL-бэкоффом
- Имена генерируются kebab-case из типов, переопределяются атрибутом `[Topic("...")]`

## 5. Семантики доставки

| Режим | Механизм |
|-------|----------|
| At-most-once | fire-and-forget, autoack |
| At-least-once | ack после обработки + ретраи (дефолт) |
| Effectively-once | Inbox-дедупликация + идемпотентные хендлеры |
| Exactly-once (Kafka) | транзакции producer + read_committed |

## 6. Потоки данных: команда с Outbox

```
HTTP → Handler → EF Core SaveChanges
                   ├── бизнес-данные
                   └── outbox_messages (та же транзакция!)
        Relay (Channel<T> push + polling fallback)
                   └── Transport → Broker → Consumers
```

## 7. Схема принятия решений при проектировании

| Вопрос | Решение AvtoBus | Почему |
|--------|-----------------|--------|
| Интерфейс vs методы-хендлеры | Оба, метод — рекомендация | Wolverine доказал удобство |
| Рефлексия vs codegen | Source Generators | AOT, скорость, диагностика на компиляции |
| Конфиг: код vs файлы | Код + профили окружений | Типобезопасность |
| Транзакции | Outbox по умолчанию при наличии DbContext | CAP доказал: это главный фикс EDA |
| Сериализация | System.Text.Json (source-gen) дефолт | Скорость, AOT |
| Версионирование | Alias + upcasters | Axon-подход |
