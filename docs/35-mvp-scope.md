# AvtoBus MVP Scope

> **Статус: Specification / scope control.** Этот документ защищает первый рабочий релиз от превращения в бесконечную платформу.

## Цель MVP

Доказать одну сквозную историю:

```text
ASP.NET endpoint
  -> scoped IMessageSession
  -> business data + Outbox in PostgreSQL transaction
  -> RabbitMQ quorum queue/topic
  -> handler with Inbox deduplication
  -> cascade event in same local transaction
  -> OpenTelemetry trace
```

## Входит в MVP

### Runtime

- .NET 10 target.
- `AvtoBus.Core` contracts и pipeline.
- Source-generated method handlers.
- `IConsumer<T>` compatibility style.
- JSON serializer через source-generated `System.Text.Json` metadata.
- InMemory transport для deterministic tests.
- RabbitMQ transport: queue commands, topic events, manual ack, quorum queue, confirms.
- EF Core PostgreSQL Outbox/Inbox.
- Immediate и durable delayed retry.
- Error/poison DLQ с rich metadata.
- OpenTelemetry traces и базовые metrics.
- Graceful drain и health checks.
- Test harness с fake time.

### Developer experience

- Один quick-start sample.
- Один e-commerce integration sample с двумя сервисами, не шестью.
- `dotnet new avtobus-worker` только после стабилизации quick-start.
- Roslyn diagnostics AVB001-AVB006.
- Документация build/test/retry/outbox/troubleshooting.

## Явно не входит в MVP

- Kafka, NATS, Redis, Azure Service Bus, SQS.
- Event Sourcing и projection daemon.
- Durable execution/Temporal-like workflows.
- State machine DSL и routing slips.
- Dashboard SPA и универсальный CLI.
- Multi-region и active-active.
- WASM plugins, AI-функции, stream DSL.
- MessagePack/Protobuf/Avro.
- Claim Check и chunking.
- Dynamic runtime topology editor.
- Exactly-once marketing claims.

Эти возможности остаются в каталоге 500 идей, но не блокируют MVP.

## Публичный пакетный набор MVP

```text
AvtoBus.Core
AvtoBus.Generators
AvtoBus.InMemory
AvtoBus.RabbitMq
AvtoBus.Outbox.EfCore
AvtoBus.Testing
AvtoBus.AspNetCore
```

Метапакет `AvtoBus` может ссылаться только на Core + Generators + InMemory. Он не должен автоматически тянуть RabbitMQ или EF Core.

## Non-functional gates

- Core dependency graph соответствует ADR-0001.
- `ValidateScopes=true` проходит.
- Все обязательные строки `34-verification-matrix.md` зелёные.
- Core + InMemory Native AOT sample публикуется без warnings.
- Нет unbounded channels на receive path.
- Нет payload или tenant id в metric labels.
- Upgrade migration N-1 -> N протестирована.

## Exit criteria

MVP объявляется готовым не по числу файлов или строк, а только когда:

1. Quick start создаётся с нуля и проходит в чистом CI runner.
2. Outbox failure matrix доказана на PostgreSQL Testcontainer.
3. RabbitMQ conformance suite зелёная.
4. Process kill между commit и ack не удваивает бизнес-эффект.
5. Документация ссылается на реальный `src/`, а не копирует расходящиеся эскизы.

## Изменение scope

Добавление новой обязательной функции требует:

- use case, который нельзя решить текущим API;
- оценки влияния на Core boundary и AOT;
- новых строк verification matrix;
- удаления функции равного объёма или явного переноса release date.