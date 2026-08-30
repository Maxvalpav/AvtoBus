# AvtoBus: дополнительные конкуренты и источники идей

Этот документ дополняет `01-market-research.md` решениями, которые в первом проходе были упомянуты вскользь или пропущены. Для каждого - что взять, чего избегать, как отразить в AvtoBus.

## Microsoft-native durable execution

### Azure Durable Task Framework (DTFx)

DTFx - open source библиотека от Microsoft, на которой построены Durable Functions и Durable Entities. Дает task orchestration, activity execution, timers, external events, sub-orchestrations, continue-as-new и entity state. Работает поверх storage providers: Azure Storage, MSSQL, Netherite (FasterKV + Event Hubs), Redis, и community providers (PostgreSQL, MongoDB).

Сильные стороны:

- Полностью .NET-native, без отдельного server.
- Прямая интеграция с Azure Functions и ASP.NET Core.
- Entity model (Durable Entities) похож на actor/single-aggregate concurrency.
- Storage provider abstraction позволяет self-host.

Слабые стороны:

- Programming model менее ergonomic, чем Temporal workflows-as-code.
- Меньше аналитических инструментов уровня Temporal Web UI.
- Исторически привязан к Azure storage mental model.

Что взять в AvtoBus:

- Идея pluggable storage provider для workflow engine.
- Durable Entities как lightweight single-key concurrency primitive поверх sagas.
- Continue-as-new для длинных workflow.

Чего избежать:

- Не копировать orchestrator context API с ручными yield/replay деталями. AvtoBus должен сохранить deterministic workflow model, но с analyzer-driven diagnostics вместо runtime traps.

### Azure Durable Functions

Durable Functions - serverless orchestration поверх DTFx. Orchestration, activity, entity, durable HTTP, fan-out/fan-in, monitor, human interaction patterns.

Что взять:

- Named patterns как cookbook: function chaining, fan-out/fan-in, async HTTP API, monitor, human interaction.
- External event wait как first-class primitive.

Чего избежать:

- Serverless-only assumptions. AvtoBus workflows должны одинаково работать в ASP.NET Core, Worker Service и Aspire.

## Actor systems и persistence

### Microsoft Orleans

Virtual actors (grains), reminders, timers, streams, grain persistence, cluster membership, placement strategies. Orleans 7+ хорошо интегрируется с .NET DI и Generic Host.

Сильные стороны:

- Virtual actor model устраняет lifecycle management.
- Reminders переживают рестарты зерен и подходят для saga timeouts.
- Stream providers могут быть Event Hubs, Kafka, in-memory.
- Placement strategies дают control над affinity.

Слабые стороны:

- Mental model "grain per entity" не всегда совпадает с message-driven EDA.
- Silo cluster operations тяжелее, чем stateless worker deployment.

Что взять:

- Reminder-like durable timers как опция для saga/workflow.
- Grain-per-aggregate как optional execution mode для event sourcing hot keys.
- Placement strategies для partition affinity.

### Akka.NET Persistence и Akka.Cluster.Sharding

Event sourcing и CQRS через persistent actors, cluster sharding, snapshots, persistence query. Akka.Persistence.Query - projection building blocks.

Что взять:

- Persistence Query API как проекционная модель: events by tag, current events, live events, sequence numbers.
- Snapshot strategy и retention.
- Sharding как entity affinity для stream/saga consumers.

Чего избежать:

- Не превращать AvtoBus в actor framework. Actor mode должен быть опцией для aggregate affinity, а не обязательной моделью.

## Autoscaling и runtime

### KEDA

KEDA - Kubernetes Event-Driven Autoscaling. ScaledObject может масштабировать deployment по RabbitMQ queue depth, Kafka consumer lag, Azure Service Bus queue length, NATS JetStream lag, PostgreSQL query и другим scalers.

Почему это важно для AvtoBus:

- Без KEDA consumer scaling требует ручной настройки replica count или HPA по custom metrics.
- AvtoBus должен публиковать lag/queue depth metrics в формате, который KEDA scaler понимает, или поставлять готовый ScaledObject manifest generator.

Что взять:

- `dotnet avto keda export --endpoint orders --min 2 --max 50` для генерации ScaledObject.
- Встроенные Prometheus metrics: `avto_endpoint_queue_depth`, `avto_endpoint_consumer_lag`, `avto_outbox_lag_seconds`.
- Health endpoint, который KEDA может использовать для zero-replica decisions.

### Azure Container Apps scale rules

Azure Container Apps имеет встроенные scale rules для Kafka, RabbitMQ, Service Bus, HTTP, TCP. AvtoBus должен документировать recommended rule templates.

## Transport и streaming детали

### RabbitMQ Streams

RabbitMQ Streams - append-only log внутри RabbitMQ с consumer offset tracking, single active consumer, super streams и filter values. Это не AMQP queue и не Kafka, а отдельная модель.

Что взять:

- Single Active Consumer для ordered processing per stream без external coordinator.
- Filter values для server-side event filtering.
- Super streams как sharded stream model.

AvtoBus capability:

```text
RabbitStream = Topic + Replay + OffsetCommit + SingleActiveConsumer + FilterValues
```

### Redpanda

Redpanda - Kafka-compatible broker с tiered storage, transforms (WASM), data transforms, schema registry built-in и меньшей operational complexity.

Что взять:

- Поддержка Redpanda как first-class Kafka-compatible transport с отдельными capability flags для tiered storage и transforms.
- Redpanda Schema Registry совместимость с AvtoBus SchemaRegistry package.

### NATS JetStream детальнее

JetStream дает durable streams, consumer with AckPolicy, MaxAckPending, pull/push consumers, stream retention, deduplication window, subject wildcards, advisory subjects.

Что взять:

- Pull consumer как default backpressure model.
- Deduplication window как native inbox для NATS transport.
- Advisory subjects как source для operational events.

### gRPC streaming как transport

gRPC server/client streaming и bidi streaming могут быть transport для request-response и event streaming между .NET services без broker.

Что взять:

- Optional `AvtoBus.Transport.Grpc` для point-to-point streaming, service discovery через Aspire/Kubernetes.
- CloudEvents over gRPC binding.

## Schema и contract standards

### Confluent Schema Registry

Avro/Protobuf/JSON Schema registry с compatibility checks, subject naming strategies, REST API. Широко используется в Kafka экосистеме.

Что взять:

- AvtoBus.SchemaRegistry должен уметь работать в двух режимах: embedded PostgreSQL store и external Confluent-compatible REST registry.
- Subject naming strategy: TopicNameStrategy, RecordNameStrategy, TopicRecordNameStrategy.

### AsyncAPI spec

AsyncAPI - спецификация для event-driven API: channels, operations, messages, schemas, servers, security, bindings per broker.

Что взять:

- `dotnet avto schemas export --format asyncapi` должен генерировать валидный AsyncAPI 3.x.
- Routing configuration должна быть источником truth для AsyncAPI channels и bindings.
- CI check: AsyncAPI diff между версиями как breaking change gate.

### CloudEvents spec детальнее

CloudEvents 1.0.2 определяет attributes: id, source, type, specversion, time, datacontenttype, dataschema, subject, extensions. Bindings: HTTP, Kafka, AMQP, NATS, MQTT, WebSockets. Binary и structured content modes.

Что взять:

- AvtoBus envelope <-> CloudEvents mapping должен быть lossless для core attributes.
- Binary mode для Kafka headers и AMQP headers.
- Structured mode для HTTP и webhook.
- Extensions namespace `avto*` для correlation, causation, tenant, schema version.
- SDK-level support без обязательной зависимости от CloudEvents SDK.

## Event ingestion из БД

### Debezium и CDC

Debezium - CDC platform для MySQL, PostgreSQL, SQL Server, MongoDB, Oracle. Публикует change events в Kafka в CloudEvents-compatible или Debezium envelope формате.

Почему важно для AvtoBus:

- Многие существующие системы не могут сразу писать в outbox. CDC bridge позволяет читать legacy DB changes как events.
- AvtoBus должен иметь `AvtoBus.Ingestion.Debezium` adapter, который normalizes Debezium envelope в AvtoBus events.

Что взять:

- Debezium envelope normalization.
- Tombstone handling для delete events.
- Schema change event handling.

### SQL Server Change Tracking и PostgreSQL logical replication

Если Kafka/Debezium слишком тяжелы, native CDC может быть lighter transport. AvtoBus может поддерживать polling или replication slot reader как ingestion source.

## Scheduler и jobs overlap

### Hangfire

Hangfire - persistent background jobs для .NET: fire-and-forget, delayed, recurring, continuations, dashboard, SQL/Redis/Mongo storage.

Что взять:

- Dashboard UX для scheduled/recurring jobs.
- Continuation jobs как inspiration для workflow step chaining.

Чего избежать:

- Не смешивать domain events и background jobs в одной модели. Jobs - operational work, events - business facts. AvtoBus должен разделять их, но позволять scheduler отправлять command/event.

### Quartz.NET

Quartz.NET - enterprise scheduler с cron, calendar intervals, misfire instructions, persistent stores, clustering.

Что взять:

- Cron и calendar interval expressions для recurring domain commands.
- Misfire instruction model для scheduled messages после downtime.

## Contract testing

### Pact

Pact - consumer-driven contract testing. Для EDA это Pact Message: consumer определяет ожидаемые event payloads, provider публикует events и проверяется contract.

Что взять:

- `AvtoBus.Testing` должен уметь export message pact files из test harness.
- CI integration: contract verification между producer и consumer services.

### SpecFlow / BDD для events

Не основной источник, но BDD-style tests для sagas и workflows могут улучшить readability.

## F#/functional inspiration

### Railway programming и F# result types

Wolverine уже поддерживает compound handlers и railway style. F# Result/Choice и computation expressions дают хороший mental model для sad path.

Что взять:

- `HandlerContinuation` и typed `ProblemDetails` как standard.
- Optional F# sample package: `AvtoBus.Samples.FSharp`.

## C# language features для API

### C# 13-15 features

- Primary constructors для handlers/services.
- Collection expressions для effects lists.
- Union types из C# 15 могут заменить `HandlerContinuation` + `ProblemDetails` в некоторых API.
- `params ReadOnlySpan<T>` для low-allocation effect lists.
- `TimeProvider` уже используется, это хорошо.

Что взять:

- Public API должен быть friendly к collection expressions: `AvtoEffects.All([effect1, effect2])`.
- Union types можно использовать в v2 API после stabilization.

## Что явно не брать

- Route DSL в стиле Apache Camel для business logic.
- Mandatory sidecar runtime как в Dapr.
- Mandatory central server как в Axon Server или Temporal Server для простых messaging scenarios.
- Actor-only model как в Akka.NET/Orleans для всех EDA use cases.
- YAML-heavy binding model без compile-time diagnostics.

## Сводная таблица дополнительных идей

| Источник | AvtoBus package или feature |
| --- | --- |
| DTFx / Durable Functions | `AvtoBus.Workflow` storage providers, continue-as-new, durable entities optional |
| Orleans | grain-per-aggregate optional mode, reminders, placement |
| Akka.Persistence | persistence query API, snapshots, sharding ideas |
| KEDA | metrics, ScaledObject export, zero-replica health |
| RabbitMQ Streams | `AvtoBus.Transport.RabbitMQ.Streams` |
| Redpanda | Kafka transport compatibility + schema registry mode |
| NATS JetStream | pull consumers, dedup window, advisories |
| gRPC streaming | `AvtoBus.Transport.Grpc` optional |
| Confluent Schema Registry | external registry adapter |
| AsyncAPI | schema export and CI diff |
| CloudEvents | lossless envelope mapping |
| Debezium | `AvtoBus.Ingestion.Debezium` |
| Hangfire/Quartz | recurring domain commands, misfire instructions |
| Pact | contract test export |
| F# railway | typed problem results and samples |
