# AvtoBus

Проектная документация EDA-фреймворка для .NET 10 и планируемой поддержки .NET 11.

> Статус: **design draft / pre-alpha**. Здесь находится спецификация и набор C#-эскизов, а не опубликованный NuGet-пакет. Код из `docs/code/` ещё не перенесён в `.csproj`, не прошёл `dotnet build`, тесты, AOT-публикацию и проверку на реальном брокере. Канонический статус: [FINAL.md](./FINAL.md).

## Начать здесь

| Документ | Назначение |
|---|---|
| [01-architecture.md](./01-architecture.md) | Архитектура и границы модулей |
| [03-core-api.md](./03-core-api.md) | Целевой публичный API |
| [22-getting-started.md](./22-getting-started.md) | Проектируемый сценарий первого запуска |
| [27-gap-analysis.md](./27-gap-analysis.md) | План превращения документации в продукт |
| [29-glossary.md](./29-glossary.md) | Термины и сокращения |
| [30-forgotten-and-bugs.md](./30-forgotten-and-bugs.md) | Известные противоречия и технический долг |
| [32-documentation-policy.md](./32-documentation-policy.md) | Правила статусов, сравнений и code snippets |
| [adr/README.md](./adr/README.md) | Индекс архитектурных решений |
| [33-wire-protocol.md](./33-wire-protocol.md) | Transport-neutral wire protocol v1 |
| [34-verification-matrix.md](./34-verification-matrix.md) | Заявления MVP и обязательные доказательства |
| [35-mvp-scope.md](./35-mvp-scope.md) | Жёсткие границы первого релиза |
| [40-release-readiness.md](./40-release-readiness.md) | Release Gate Board: порядок PR-ов до MVP |
| [FINAL.md](./FINAL.md) | Проверенный статус артефактов |

## Статусы документов

- **Specification**: целевое поведение и API, ещё не гарантия реализации.
- **Design draft**: архитектурный эскиз, который может измениться.
- **Code sketch**: пример реализации; может содержать пропущенные `using`, зависимости и несовместимые фрагменты.
- **Verified**: перенесено в исходный проект и подтверждено сборкой/тестами. Сейчас таких C#-модулей нет.

## Архитектура и исследование

| Файл | Статус | Содержание |
|---|---|---|
| [01-architecture.md](./01-architecture.md) | Specification | Слои, пакеты, пайплайн, доставка |
| [02-competitors.md](./02-competitors.md) | Research draft | Сравнение решений на семи языках |
| [03-core-api.md](./03-core-api.md) | Specification | `IBus`, контексты, handlers, middleware |
| [19-roadmap.md](./19-roadmap.md) | Planning | Этапы реализации |
| [20-benchmarks.md](./20-benchmarks.md) | Target SLO | Методика и целевые, не измеренные значения |
| [21-comparison-matrix.md](./21-comparison-matrix.md) | Target comparison | Планируемые, а не текущие возможности AvtoBus |
| [28-project-structure.md](./28-project-structure.md) | Design draft | Предлагаемые solution, `.csproj`, CI/CD |
| [31-project-meta.md](./31-project-meta.md) | Templates | Шаблоны README, LICENSE, SECURITY, ADR |
| [33-wire-protocol.md](./33-wire-protocol.md) | Specification draft | Wire envelope, headers, compatibility, signing |
| [34-verification-matrix.md](./34-verification-matrix.md) | Quality gate | Claim-to-test матрица MVP |
| [35-mvp-scope.md](./35-mvp-scope.md) | Specification | Что входит и не входит в MVP |
| [36-threat-model.md](./36-threat-model.md) | Specification draft | STRIDE threat model, PII redact, envelope encryption |
| [37-production-playbook.md](./37-production-playbook.md) | Specification draft | SRE playbook: алерты, инциденты, DR, outbox claims |
| [38-lifecycle-state-machines.md](./38-lifecycle-state-machines.md) | Specification draft | Стейт-машины Envelope, Outbox, Saga и Consumer |
| [39-performance-tuning-aot.md](./39-performance-tuning-aot.md) | Specification draft | Zero-alloc практики, MemoryPool, Native AOT, tuning |
| [40-release-readiness.md](./40-release-readiness.md) | Planning | Канонический PR-путь, release labels, reviewer checklist |

## Архитектурные решения

| ADR | Статус | Решение |
|---|---|---|
| [ADR-0001](./adr/0001-core-boundaries.md) | Proposed | Границы Core |
| [ADR-0002](./adr/0002-bus-lifetime-and-uow.md) | Proposed | `IBus`, `IMessageSession`, Unit of Work |
| [ADR-0003](./adr/0003-delivery-semantics.md) | Proposed | Семантики доставки |
| [ADR-0004](./adr/0004-handler-contract.md) | Proposed | Handler contract и каскады |
| [ADR-0005](./adr/0005-request-reply.md) | Proposed | Request/Reply lifecycle |

## Каталог 500 идей

| Диапазон | Файл | Тема |
|---:|---|---|
| 1-50 | [04-ideas-001-050-core.md](./04-ideas-001-050-core.md) | Ядро и API |
| 51-100 | [05-ideas-051-100-transports.md](./05-ideas-051-100-transports.md) | Транспорты |
| 101-150 | [06-ideas-101-150-contracts.md](./06-ideas-101-150-contracts.md) | Контракты и схемы |
| 151-200 | [07-ideas-151-200-reliability.md](./07-ideas-151-200-reliability.md) | Надёжность |
| 201-250 | [08-ideas-201-250-sagas.md](./08-ideas-201-250-sagas.md) | Саги и workflow |
| 251-300 | [09-ideas-251-300-eventsourcing.md](./09-ideas-251-300-eventsourcing.md) | Event Sourcing и CQRS |
| 301-350 | [10-ideas-301-350-observability.md](./10-ideas-301-350-observability.md) | Наблюдаемость и тесты |
| 351-400 | [11-ideas-351-400-performance.md](./11-ideas-351-400-performance.md) | Производительность |
| 401-450 | [12-ideas-401-450-devex.md](./12-ideas-401-450-devex.md) | DevEx и инструменты |
| 451-500 | [13-ideas-451-500-security-cloud.md](./13-ideas-451-500-security-cloud.md) | Безопасность и cloud-native |

## Обзорные эскизы

Файлы `14-18` объясняют устройство подсистем и частично дублируют `docs/code/`. При расхождении приоритет у `01-architecture.md` и `03-core-api.md`, пока не появится компилируемый `src/`.

- [14-implementation-core.md](./14-implementation-core.md)
- [15-implementation-outbox.md](./15-implementation-outbox.md)
- [16-implementation-sourcegen.md](./16-implementation-sourcegen.md)
- [17-implementation-sagas.md](./17-implementation-sagas.md)
- [18-implementation-transports.md](./18-implementation-transports.md)
- [23-implementation-dashboard.md](./23-implementation-dashboard.md)
- [24-implementation-testing.md](./24-implementation-testing.md)
- [25-implementation-cli.md](./25-implementation-cli.md)
- [26-example-end2end.md](./26-example-end2end.md)

## C#-эскизы

Все файлы этого раздела имеют статус **Code sketch / unverified**.

| Файл | Область |
|---|---|
| [code/01-core-types.md](./code/01-core-types.md) | Envelope, options, result, metrics |
| [code/02-core-interfaces.md](./code/02-core-interfaces.md) | `IBus`, context, pipeline |
| [code/03-core-defaultbus.md](./code/03-core-defaultbus.md) | Bus и host |
| [code/04-core-middleware.md](./code/04-core-middleware.md) | Стандартные middleware |
| [code/05-transport-interfaces.md](./code/05-transport-interfaces.md) | Transport API и InMemory |
| [code/06-outbox.md](./code/06-outbox.md) | EF Core Outbox/Inbox |
| [code/07-sagas.md](./code/07-sagas.md) | Саги |
| [code/08-extensions.md](./code/08-extensions.md) | DI, dashboard, testing |
| [code/09-source-generator.md](./code/09-source-generator.md) | Source Generator |
| [code/10-rabbitmq-transport.md](./code/10-rabbitmq-transport.md) | RabbitMQ |
| [code/11-glue-code.md](./code/11-glue-code.md) | Контракты и fallback dispatch |
| [code/12-eventsourcing.md](./code/12-eventsourcing.md) | Event Store и проекции |
| [code/13-scheduling.md](./code/13-scheduling.md) | Cron и scheduling |
| [code/14-reliability-glue.md](./code/14-reliability-glue.md) | UoW, replies, DLQ, local queues |
| [code/15-hosting-config.md](./code/15-hosting-config.md) | Hosting, configuration, OTel |
| [code/16-test-harness-fix.md](./code/16-test-harness-fix.md) | Исправление A7: event-driven `DrainAsync`, реальный старт host |

## Принципы проекта

1. Простое по умолчанию, расширяемое при необходимости.
2. Source generation для AOT; reflection fallback только как явно ограниченный режим.
3. At-least-once доставка и effectively-once обработка через Inbox/идемпотентность.
4. Outbox связан с бизнес-транзакцией, а не просто существует как таблица.
5. Наблюдаемость и тестируемость входят в контракт ядра.
6. Транспортные особенности не протекают в доменные handlers без явного выбора.

## Целевой API

```csharp
builder.Services.AddAvtoBus(bus =>
{
    bus.UseRabbitMq("amqp://localhost");
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.UseOutbox<AppDbContext>();
});

public static class OrderHandlers
{
    public static async Task<OrderPlaced> Handle(
        PlaceOrder command,
        IOrderRepository repository,
        CancellationToken cancellationToken)
    {
        var order = Order.Create(command.CustomerId, command.Items);
        await repository.Save(order, cancellationToken);
        return new OrderPlaced(order.Id);
    }
}
```

Этот пример описывает желаемый API. Он станет quick start только после появления компилируемого sample и CI-проверки.

## Реальные сэмплы

Компилируемые примеры лежат в `samples/` и проверяются сборкой (не входят в основной `AvtoBus.slnx`):

| Сэмпл | Назначение |
|---|---|
| `samples/AvtoBus.QuickStart` | ASP.NET Core + RabbitMQ + outbox + dashboard |
| `samples/AvtoBus.AotSample` | Native AOT worker на InMemory |
| `samples/AvtoBus.AotSample.RabbitMq` | Native AOT worker на RabbitMQ |
| `samples/AvtoBus.Logistics` | 30 логистических микросервисов (модульный монолит, идея 27) — отдельное решение `AvtoBus.Logistics.slnx`, детали в `samples/AvtoBus.Logistics/README.md` |