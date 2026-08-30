# AvtoBus: анализ конкурентов и лучших идей

Документ охватывает основные конкурирующие классы решений: .NET service bus frameworks, distributed runtimes, workflow engines, stream processing, CQRS/event sourcing, actor systems, job queues и брокерные SDK. Слово "все" в реальном рынке означает не каждый GitHub-пакет, а все значимые категории и ведущие решения, из которых стоит брать архитектурные идеи.

## Короткий рейтинг идей

| Источник | Что взять в AvtoBus | Чего избегать |
| --- | --- | --- |
| Wolverine | Source generation, convention handlers, pure functions, cascading messages, mediator + messaging в одном фреймворке | жесткое ограничение только на некоторые сценарии, недостаточная enterprise tooling глубина на старте |
| MassTransit | зрелые consumers, retries, redelivery, outbox, sagas, test harness, topology, multi-transport | слишком много runtime wiring и ceremony для простых handler scenarios |
| NServiceBus | recoverability, outbox semantics, endpoint discipline, operational tooling, audit/error queues | commercial lock-in, тяжелая endpoint model для modern modular monolith |
| CAP | простой local message table, dashboard, guaranteed delivery, wildcard topics, consumer groups | attribute-heavy subscription, меньше мощи для complex workflows |
| Brighter | command processor, outbox/inbox thinking, resilience pipelines, sweeper process | базовые классы и attribute pipeline могут быть менее ergonomic |
| Rebus | simplicity, small API surface, easy routing, sagas | меньше enterprise-level functionality, outbox не такой центральный |
| SlimMessageBus | lightweight facade, child buses, many providers | меньше opinionated reliability platform |
| Dapr | sidecar portability, cross-language APIs, components, CloudEvents, state/secrets/workflow blocks | sidecar overhead, runtime dependency, less C# domain ergonomics |
| Temporal | durable workflows-as-code, replay, timers, signals, queries, resilient long-running processes | отдельная platform dependency, deterministic coding restrictions |
| Kafka Streams | state stores, exactly-once v2 inside Kafka, event-time windows, stream-table duality | Kafka-only world, Java-first model |
| Spring Cloud Stream | binder abstraction, function model, consumer groups, partitions | YAML-heavy config, Java/Spring coupling |
| Axon | CQRS/event sourcing, aggregates, command/event gateways, sagas, projections | Java/Spring/Axon Server assumptions |
| Watermill | Go struct-based CQRS, router middleware, event handler groups for ordering | ручная wiring complexity в больших системах |
| Apache Camel | enterprise integration patterns catalog, rich connectors | route DSL can become integration spaghetti |

## .NET конкуренты

### MassTransit

MassTransit позиционируется как distributed application framework for .NET для reliable, testable and observable message-based systems. Поддерживает RabbitMQ, Azure Service Bus, Amazon SQS, ActiveMQ, SQL transport, Kafka/Event Hub riders, message contracts, consumers, retries, redelivery, outbox, scheduling, workflows, sagas and unit test harness.

Сильные стороны:

- Очень зрелая экосистема .NET messaging.
- Богатая reliability pipeline: retries, delayed redelivery, outbox, fault handling, rate limiting, partitioning.
- Saga state machines, routing slips, job consumers.
- Хорошая testing story через test harness.
- Подходит для production microservices и enterprise integration.

Слабые стороны:

- Consumer интерфейсы и framework context добавляют ceremony.
- In-process mediator и distributed messaging не так естественно едины, как в Wolverine.
- Kafka/Event Hub представлены как riders, потому что stream semantics отличаются от queue broker semantics.

Что взять:

- Test harness как first-class API.
- Explicit topology и transport-specific настройки.
- Saga workflows и routing slips, но с более простым source-generated API.
- Fault model: fault events, dead-letter, retry policies, observability.

### NServiceBus

NServiceBus задает сильную enterprise-дисциплину вокруг endpoint model, recoverability, transaction modes, sagas, outbox и operational monitoring. В документации явно описаны consistency проблемы: ghost messages, zombie records, idempotency, transaction scope, sends atomic with receive, receive only, unreliable mode. Outbox дает exactly-once message processing semantics без distributed transactions, если outbox хранится в той же базе, что business data.

Сильные стороны:

- Лучшие объяснения и tooling вокруг reliability/recoverability.
- Mature sagas и endpoint patterns.
- Error queues, audit queues, ServiceControl/ServicePulse style operations.
- Четкая transactional semantics матрица по transport capabilities.

Слабые стороны:

- Коммерческая лицензия для production.
- Более тяжелый mental model и больше ceremony.
- Endpoint-centric approach не всегда удобен для modular monolith и serverless style.

Что взять:

- Явную терминологию consistency hazards: lost send, phantom/ghost message, zombie record, duplicate processing.
- Infrastructure-level idempotency через inbox/outbox.
- Recoverability dashboard и операционные queues.
- Saga concurrency rules и optimistic concurrency by default.

### Wolverine

Wolverine объединяет mediator и asynchronous messaging. Главное отличие - handlers могут быть plain C# methods без интерфейсов, base classes и attributes. Wolverine использует runtime code generation для optimized execution pipelines, method injection, cascading messages и selective middleware per message type.

Сильные стороны:

- Самый современный handler model в .NET.
- Pure function style и return values as messages.
- Runtime/compile-time code generation вместо тяжелого runtime interface dispatch.
- Local queues + distributed transports + outbox в одной модели.
- Interop с MassTransit/NServiceBus для RabbitMQ, Azure Service Bus, Amazon SQS/SNS, Kafka CloudEvents.

Слабые стороны:

- Concrete message requirement может ломать interface-based ecosystems.
- Code generation требует хорошей diagnostic story.
- Комьюнити меньше, чем у MassTransit.

Что взять:

- Source-generated static handler pipelines как базовый путь AvtoBus.
- Method parameter injection.
- Cascading messages and effects.
- Handler discovery by convention.
- Migration shims для MediatR/MassTransit/NServiceBus.

### CAP

CAP - .NET library для distributed transactions и event bus integration через local message table/outbox. Поддерживает RabbitMQ, Kafka, Azure Service Bus, Amazon SQS, NATS, Redis Streams, Pulsar, SQL Server, PostgreSQL, MySQL, MongoDB, dashboard, delayed messages, wildcard subscriptions, consumer groups, serial/parallel processing, backpressure и OpenTelemetry.

Сильные стороны:

- Очень понятная модель: database transaction + message table + broker publish.
- Быстрый onboarding для ASP.NET Core/EF Core.
- Dashboard и manual retry из коробки.
- Поддержка wildcard topics и consumer groups.

Слабые стороны:

- Attribute subscription хуже масштабируется на сложные bounded contexts.
- Меньше возможностей для complex workflow/saga orchestration.
- EventBus-centric, а не full distributed application framework.

Что взять:

- Local message table как simple default outbox.
- Dashboard-first operational UX.
- Consumer groups и topic wildcard support.
- Backpressure controls.

### Rebus

Rebus - lean service bus for .NET, практичный для простых asynchronous messaging scenarios. Типовой стиль: `IHandleMessages<T>`, type-based routing, RabbitMQ/Azure Service Bus/SQL transport, sagas, timeout storage, retry strategy, auditing.

Сильные стороны:

- Простота и малая поверхность API.
- Быстрое внедрение.
- Sagas и timeouts без огромной платформы.

Слабые стороны:

- Меньше встроенной observability и advanced operations.
- Меньше ambition по unifying mediator, workflows, event sourcing и streams.

Что взять:

- Простые defaults.
- Легкая migration path для маленьких сервисов.
- Small core, plugins вокруг.

### Brighter

Paramore Brighter - command processor, dispatcher and task queue с strong outbox/inbox patterns. Документация хорошо объясняет lost send и phantom send. Outbox sweeper eventually dispatches messages, а consumers должны иметь inbox/idempotency, потому что outbox дает guaranteed at-least-once delivery.

Сильные стороны:

- Четкая command processor модель.
- Outbox/inbox как архитектурная основа.
- Resilience pipelines.
- Deposit and clear outbox flow.

Слабые стороны:

- Handler base classes и attribute pipeline менее современны, чем pure function handlers.
- Требуется аккуратная настройка, чтобы получить транзакционность.

Что взять:

- Outbox sweeper model and operational replay.
- Inbox dedupe as first-class consumer protection.
- Command processor abstractions, но без обязательного inheritance.

### SlimMessageBus

SlimMessageBus - lightweight .NET message bus facade с Kafka, RabbitMQ, Azure EventHubs, MQTT, Redis Pub/Sub, Azure Service Bus и child bus configuration. Поддерживает pub/sub и request-response.

Сильные стороны:

- Легкий facade.
- Много providers.
- Хорош для приложений, где нужен thin abstraction layer.

Слабые стороны:

- Не является полноценной reliability/workflow platform.

Что взять:

- Child buses для modular monolith и multi-transport scenarios.
- Простую facade-ergonomics.

### MediatR, Martinothamar.Mediator, MessagePipe

Эти библиотеки покрывают in-process mediator/eventing. Они не решают distributed reliability, outbox, broker topology и workflow durability.

Что взять:

- Простота command/query API.
- Source generator подход из высокопроизводительных mediator packages.

Чего избегать:

- Нельзя выдавать in-process publish за distributed EDA.

### Marten, EventFlow, EventStoreDB, Orleans, Akka.NET, Proto.Actor

Это не прямые service bus конкуренты, но важные источники идей.

- Marten: PostgreSQL document DB/event store, projections, strong integration with Wolverine.
- EventFlow: CQRS/event sourcing для .NET.
- EventStoreDB: специализированное event store хранилище.
- Orleans/Akka.NET/Proto.Actor: actor concurrency, reminders, placement, stateful entities.

Что взять:

- Actor-like single aggregate concurrency для hot keys.
- Event store abstraction и projection rebuild tools.
- Placement/partitioning ideas для entity affinity.

## Другие языки и платформы

### Java: Spring Cloud Stream

Spring Cloud Stream строит scalable event-driven microservices через binder abstraction. Поддерживает RabbitMQ, Kafka, Kafka Streams, Pulsar, Kinesis, Pub/Sub, Azure Event Hubs, Azure Service Bus и другие binders. Основные blocks: Destination Binders, Bindings, Message. Есть persistent pub/sub semantics, consumer groups и stateful partitions.

Что взять:

- Binder abstraction с декларативной binding model.
- Consumer groups и partitions как часть public model.
- Function style for handlers.

Чего избегать:

- YAML-heavy magic.
- Слишком много implicit conventions без compiler diagnostics.

### Java: Kafka Streams

Kafka Streams - lightweight client library for processing data in Kafka. Идеи: topology graph, event time, processing time, ingestion time, stream-table duality, state stores, windowing, out-of-order handling, exactly-once v2, interactive queries.

Что взять:

- Stateful processors with local state stores.
- Event-time windows and grace periods.
- Stream-table duality for projections/read models.
- Exactly-once effect inside Kafka pipeline where broker supports it.

Чего избегать:

- Не превращать AvtoBus в Kafka-only framework.

### Java: Axon Framework

Axon дает CQRS/event sourcing, commands, events, aggregates, projections, sagas и Axon Server integration.

Что взять:

- Aggregate command handling and event applying.
- Event sourcing handlers и projections as first-class.
- Saga/process manager model.

Чего избегать:

- Сильная platform dependency на один server.

### Java: Apache Camel

Apache Camel известен enterprise integration patterns и большим каталогом connectors.

Что взять:

- Enterprise Integration Patterns as named recipes.
- Connectors marketplace.

Чего избегать:

- Route DSL spaghetti, когда business logic теряется в integration routes.

### Go: Watermill

Watermill дает pub/sub router, middleware, CQRS commands/events as Go structs, event handler groups for ordering, transport implementations. CQRS can be partial, можно использовать только event part.

Что взять:

- Struct/record-first events.
- Router middleware simplicity.
- Event handler groups sharing one subscriber to preserve ordering.

### Go/Java/TypeScript/Python: Temporal

Temporal - durable execution platform. Workflows are code, state is event history, failures recover by replay. .NET SDK поддерживает workflows, activities, workers, signals, queries, cancellation, durable timers, deterministic scheduling.

Что взять:

- Durable timers, signals, queries.
- Workflow history replay.
- Activities as external side effects.
- Static analyzers for deterministic workflow restrictions.

Чего избегать:

- Полная зависимость от отдельного Temporal-like server для простых sagas.

### Dapr

Dapr - portable event-driven runtime with sidecar architecture and building blocks: service invocation, pub/sub, workflow, state, bindings, actors, secrets, configuration, distributed lock, jobs, observability, security. Поддерживает HTTP/gRPC APIs и SDKs для Go, Java, JavaScript, .NET, PHP, Python.

Что взять:

- Pluggable components and language neutrality.
- CloudEvents as interop default.
- Sidecar adapter as optional AvtoBus transport, not mandatory runtime.
- Dapr component manifests import/export.

### Python: Celery, Dramatiq, Faust

- Celery: mature task queues, retries, schedules, workers.
- Dramatiq: simpler background jobs.
- Faust: Kafka stream processing inspired by Kafka Streams.

Что взять:

- Operational worker UX.
- Scheduling and retry visibility.
- Simple task ergonomics for background work.

Чего избегать:

- Treating all events as tasks. Domain events need contracts, idempotency and versioning.

### Node.js: NestJS microservices, BullMQ, Moleculer

- NestJS gives decorators and transport abstraction.
- BullMQ gives Redis-based jobs, delays, retries, repeatables, priorities.
- Moleculer gives service broker model.

Что взять:

- Developer experience for quick registration.
- Job priority/delay patterns.

Чего избегать:

- Decorator magic without strong compile-time model.

### Rust and NATS ecosystem

Rust/NATS/JetStream ecosystems emphasize low latency, backpressure, pull consumers, durable consumers, subject wildcards.

Что взять:

- Pull-based backpressure.
- Subject routing for lightweight event topics.
- High-throughput zero-copy oriented payload handling where possible.

## Брокеры как конкуренты архитектуры

AvtoBus не должен прятать различия брокеров. Нужно явно моделировать capabilities.

| Broker/runtime | Сильная сторона | Ограничение | AvtoBus capability |
| --- | --- | --- | --- |
| RabbitMQ | queues, routing, exchanges, delayed/retry patterns, quorum queues | не stream processing engine | queue, topic-like routing, delayed redelivery, dead-letter |
| Kafka/Redpanda | partitions, ordering per key, replay, compacted topics, streams | command queues и request-response менее естественны | stream, partitioned event log, replay, stateful processors |
| NATS JetStream | lightweight pub/sub, durable streams, pull consumers | меньше enterprise tooling, чем Kafka | low-latency stream/queue, pull backpressure |
| Azure Service Bus | enterprise queues/topics, sessions, scheduled messages, DLQ | cloud-specific | sessions, scheduled, transaction-like send receive capabilities |
| Azure Event Hubs | high-throughput event ingestion | не command bus | event stream ingestion |
| AWS SQS/SNS | managed queue/fanout | ordering/fifo specifics, visibility timeout semantics | queue/fanout, fifo groups |
| EventBridge | event routing SaaS integration | latency/cost, not high-throughput stream | external event routing |
| PostgreSQL/SQL Server transport | simple deploy, transactional with app DB | scale ceiling compared to brokers | local durable transport and testing |
| Redis Streams | simple stream groups | persistence/ops tradeoffs | lightweight stream transport |
| Pulsar | multi-tenant topics, geo-replication | operational complexity | high-scale topic transport |

## Рынковый gap для AvtoBus

На рынке есть зрелые части, но мало решений, которые одновременно дают:

- Source-generated pure handlers like Wolverine.
- Enterprise recoverability like NServiceBus.
- Mature multi-transport and test harness like MassTransit.
- Simple outbox/dashboard like CAP.
- Durable workflows like Temporal without forcing every use case into external workflow server.
- Kafka-style stream processing in .NET without leaving ASP.NET Core hosting model.
- CloudEvents/Dapr interop for polyglot architectures.
- Native AOT/trimming-ready ASP.NET Core 10/11 experience.

Именно этот gap должен закрыть AvtoBus.

## Final recommendation

Лучшая архитектурная ставка: делать AvtoBus как source-generated EDA application framework, где core abstractions не зависят от broker, но каждый transport exposes capabilities. Reliability и observability должны быть default. Workflows, event sourcing и stream processing должны быть отдельными пакетами, но построены на том же envelope, routing, outbox/inbox и telemetry core.

## См. также

Более глубокий разбор дополнительных конкурентов и стандартов, которые не вошли в этот документ полностью: DTFx, Durable Functions, Orleans, Akka.Persistence, KEDA, RabbitMQ Streams, Redpanda, NATS JetStream, gRPC streaming, Confluent Schema Registry, AsyncAPI, CloudEvents, Debezium, Hangfire, Quartz, Pact, F# railway style - см. [06-additional-competitors.md](06-additional-competitors.md).
