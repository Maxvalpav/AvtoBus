# AvtoBus: roadmap, пакеты, MVP и критерии качества

## Product vision

AvtoBus должен стать стандартным EDA framework для ASP.NET Core 10/11 приложений, который можно использовать в трех масштабах:

- Modular monolith: local queues, in-process mediator, outbox, projections.
- Microservices: RabbitMQ/Azure Service Bus/Kafka/NATS, inbox/outbox, sagas, dashboard.
- Event platform: event sourcing, stream processing, durable workflows, schema registry, replay and operations.

## Package layout

### Core packages

| Package | Purpose |
| --- | --- |
| `AvtoBus.Abstractions` | message contracts, envelope abstractions, handler attributes/interfaces where needed |
| `AvtoBus.Core` | runtime, routing, policy graph, envelope pipeline, telemetry abstractions |
| `AvtoBus.Hosting.AspNetCore` | DI, hosted services, health checks, Minimal API helpers, dashboard endpoint mapping |
| `AvtoBus.SourceGeneration` | generated handler registry, serializers, route diagnostics |
| `AvtoBus.Analyzers` | compile-time diagnostics for contracts, handlers, workflows and schemas |
| `AvtoBus.Testing` | test host, in-memory transport, harness, deterministic scheduler |

### Reliability packages

| Package | Purpose |
| --- | --- |
| `AvtoBus.Durability.EFCore` | EF Core outbox/inbox/saga/workflow store |
| `AvtoBus.Durability.PostgreSql` | optimized PostgreSQL store with skip locked, partitioning, advisory locks |
| `AvtoBus.Durability.SqlServer` | optimized SQL Server store with locking hints |
| `AvtoBus.Durability.MongoDb` | document durability store |

### Transport packages

| Package | Purpose |
| --- | --- |
| `AvtoBus.Transport.RabbitMQ` | queues, exchanges, quorum queues, delayed redelivery, DLX |
| `AvtoBus.Transport.Kafka` | topics, partitions, consumer groups, CloudEvents, transactions where possible |
| `AvtoBus.Transport.Nats` | NATS Core and JetStream, pull consumers, durable streams |
| `AvtoBus.Transport.AzureServiceBus` | queues, topics, sessions, scheduled delivery, DLQ |
| `AvtoBus.Transport.AzureEventHubs` | high-throughput event ingestion |
| `AvtoBus.Transport.Aws` | SQS, SNS, EventBridge, Kinesis where needed |
| `AvtoBus.Transport.RedisStreams` | lightweight stream transport |
| `AvtoBus.Transport.Dapr` | Dapr Pub/Sub bridge and CloudEvents mapping |
| `AvtoBus.Transport.Sql` | PostgreSQL/SQL Server as simple durable transport |

### Advanced packages

| Package | Purpose |
| --- | --- |
| `AvtoBus.Workflow` | durable workflow engine, timers, signals, queries, activities |
| `AvtoBus.EventSourcing` | event store abstractions, aggregate support, snapshots, upcasters |
| `AvtoBus.Projections` | projection runtime, checkpointing, rebuilds |
| `AvtoBus.Streams` | stream processing DSL, windows, state stores |
| `AvtoBus.SchemaRegistry` | schema store, compatibility checks, AsyncAPI/JSON Schema export |
| `AvtoBus.Dashboard` | operations UI |
| `AvtoBus.Aspire` | Aspire integration, local resources, dashboard defaults |
| `AvtoBus.Cli` | diagnostics, schema export, replay, projection rebuild |

## MVP scope

MVP should be narrow, but production-grade.

### MVP 1: Core messaging

- `IAvtoBus.SendAsync`, `PublishAsync`, `InvokeAsync`, `ScheduleAsync`.
- Concrete record messages.
- Static method handlers with method injection.
- Source-generated handler registry.
- System.Text.Json serialization.
- In-memory transport for tests.
- RabbitMQ transport.
- EF Core durability for outbox/inbox.
- Basic retry, scheduled retry, dead-letter.
- OpenTelemetry traces/metrics.
- Test harness.

Success criteria:

- Build a real Orders + Billing + Inventory sample.
- Survive process crash after DB commit and before broker publish.
- Deduplicate redelivered messages.
- Test harness can assert published/sent messages.
- Analyzer warns when a routed integration event has no schema version.

### MVP 2: Operations

- Dashboard with routes, outbox, inbox, dead letters, endpoint health.
- CLI: `routes`, `outbox stats`, `deadletter list`, `deadletter replay`.
- Health checks for outbox lag and transport connectivity.
- AsyncAPI/JSON Schema export.
- PostgreSQL optimized store.

Success criteria:

- Operator can identify and replay a dead-letter message.
- Route graph is visible from app startup and CLI.
- Schema breaking change fails CI.

### MVP 3: Sagas

- Saga base class or pure saga methods.
- Correlation functions.
- EF/PostgreSQL saga persistence.
- Scheduled timeouts.
- Optimistic concurrency.
- Saga state visible in dashboard.

Success criteria:

- Order fulfillment saga sample with payment, inventory, shipping and timeout compensation.
- Concurrent messages for same saga do not corrupt state.

### MVP 4: Kafka and streams foundation

- Kafka transport with CloudEvents.
- Partition key routing.
- Consumer group management.
- Projection checkpoint from Kafka topic.
- Basic stream DSL: map/filter/window/aggregate/publish.

Success criteria:

- Process 100k+ messages/minute in sample on developer hardware.
- Preserve ordering per key.
- Projection lag metrics are correct.

### MVP 5: Durable workflows

- Workflow instances and history.
- Activities.
- Durable timers.
- Signals and queries.
- Workflow deterministic analyzer.
- Continue-as-new.

Success criteria:

- Workflow survives process crash mid-activity and resumes.
- `Task.Delay`, `DateTime.UtcNow`, random and direct I/O inside workflow produce analyzer errors.

## Version plan

### 0.1 preview

- Abstractions and core runtime.
- In-memory transport.
- Source generator prototype.
- Basic ASP.NET Core registration.
- Minimal test harness.

### 0.2 preview

- RabbitMQ transport.
- EF Core outbox/inbox.
- Retry/dead-letter.
- OpenTelemetry ActivitySource/Meter.

### 0.3 preview

- PostgreSQL store.
- Dashboard alpha.
- CLI alpha.
- Schema registry alpha.

### 0.4 preview

- Sagas.
- Scheduled messages.
- Optimistic concurrency and partitioned endpoints.

### 0.5 preview

- Kafka transport.
- CloudEvents interop.
- Projections and checkpointing.

### 0.6 preview

- Durable workflow alpha.
- Workflow analyzer.
- Activities/timers/signals/queries.

### 1.0 stable

Criteria for moving from preview to stable:

- API freeze: no breaking changes в `AvtoBus.Abstractions` и `AvtoBus.Core` без ADR.
- Все critical metrics документированы и протестированы (см. `10-observability-otel-chaos-backfill.md`).
- Production-ready docs: 10 документов покрыты, FAQ закрывает top-10 issues, migration guides.
- Benchmarks must-pass: latency/throughput targets hit на reference hardware.
- SLO для failure matrix hit: chaos tests проходят с 0% data loss и bounded recovery time.
- Security review проведён: PII masking, mTLS, dashboard RBAC, audit log.
- По крайней мере 2 production deployments на .NET 10 с non-trivial traffic.
- Все ADR-ы из `08` зафиксированы.

Включает:

- Core messaging stable.
- RabbitMQ, Kafka и PostgreSQL/EF Core stores stable.
- Dashboard and CLI production-ready.
- Sagas stable.
- Workflow может оставаться preview если не fully hardened.
- Documentation, samples, migration guides.

## Performance targets

Benchmarks must be public и repeatable через BenchmarkDotNet + Testcontainers + dotnet avto bench run.

### Methodology

- Hardware: 4 vCPU / 8 GB RAM baseline. Reference: AWS c5.xlarge или Azure D4s_v5.
- Runtime: .NET 10 release build, default GC, server GC.
- Разогрев: 30 секунд, измерение 60 секунд, не менее 5 итераций.
- Метрики: p50/p95/p99 latency, throughput (msg/sec), allocations (B/op, allocs/op), GC pause.
- Сравнение с baseline (MassTransit, Wolverine, NServiceBus) на тех же сценариях.
- Benchmarks выкладываются в `avtobus-bench` repository с per-commit history.

### Handler pipeline

| Scenario | Target |
| --- | --- |
| Local in-process command без I/O | < 10 us p50 после warmup |
| Generated handler invocation overhead | near direct method call, no reflection in hot path |
| Allocation per local message | < 2 KB baseline, target < 512 B for optimized path |
| Static handler с 5 dependencies | < 15 us p50, < 2 KB allocations |

### Outbox dispatcher

| Scenario | Target |
| --- | --- |
| PostgreSQL batch dispatch | 10k+ messages/sec per dispatcher |
| SQL Server batch dispatch | 5k+ messages/sec per dispatcher |
| Lock contention | safe multi-dispatcher with skip locked/advisory lock |
| Lag metric | p99 accurate within one polling interval |
| Outbox insert (batched 100) | < 5 ms p99 |
| Dispatcher heartbeat lock extension | < 1 ms p99 |

### Transport

| Scenario | Target |
| --- | --- |
| RabbitMQ simple publish/consume | 50k+ msg/min на reference hardware |
| Kafka keyed event consume | 100k+ msg/min на reference hardware |
| NATS JetStream pub/sub | 80k+ msg/min |
| In-memory test transport | deterministic и быстрее real time |

### Reliability (chaos matrix)

| Failure | Expected result | Validation |
| --- | --- | --- |
| crash after DB commit before broker send | outbox dispatches after restart | chaos test в CI |
| crash after handler commit before ack | inbox dedupes redelivery | chaos test в CI |
| broker unavailable | outbox lag grows, app health degrades, no message loss | chaos test в CI |
| poison message | retries exhausted, dead-letter with reason | chaos test в CI |
| schema breaking change | analyzer/CI failure before deploy | integration test |
| 50% packet loss to broker | retries handle, lag растёт пропорционально | toxiproxy test |
| Postgres failover | dispatcher reconnect, no message loss | chaos-mesh test |

## Quality gates

### Automated tests

- Unit tests for routing, policies, envelope mapping, schema compatibility.
- Integration tests with Testcontainers for RabbitMQ, Kafka, PostgreSQL, SQL Server.
- Crash consistency tests with forced process kill at failure points.
- Concurrency tests for sagas and partitioned consumers.
- Replay tests for dead letters and event sourcing.
- AOT/trimming sample build.

### Compatibility matrix

| Area | Must support |
| --- | --- |
| Runtime | .NET 10 LTS, .NET 11 preview/current |
| Hosting | ASP.NET Core, Worker Service, Aspire AppHost |
| Serialization | System.Text.Json first, Protobuf optional |
| Observability | OpenTelemetry, ILogger, Meter, ActivitySource |
| Databases | PostgreSQL and SQL Server first, EF Core generic |
| Brokers | RabbitMQ and Kafka first, NATS/Azure next |

### Security gates

- Payload size limits.
- Header size limits.
- PII masking tests.
- Dashboard authorization tests.
- Replay audit log.
- Message signing/encryption tests if feature enabled.

## Migration strategy

### From MediatR

1. Add AvtoBus and use local in-process handlers.
2. Replace `IMediator.Send` with `IAvtoBus.InvokeAsync`.
3. Convert `INotification` events to local/domain events.
4. Move integration events to durable outbox.

### From MassTransit

1. Add AvtoBus alongside MassTransit.
2. Enable MassTransit envelope interop per endpoint.
3. Convert new handlers to pure AvtoBus handlers.
4. Replace consumer context publish with returned effects.
5. Migrate sagas last.

### From NServiceBus

1. Keep existing endpoints.
2. Enable NServiceBus header interop for shared queues/topics.
3. Convert interface message contracts to concrete records with aliases.
4. Map recoverability policies.
5. Move saga data to AvtoBus saga state.

### From CAP

1. Reuse database outbox idea.
2. Import CAP topics into AvtoBus routing.
3. Replace `[CapSubscribe]` with generated handlers.
4. Keep topic names stable for external compatibility.

### From Dapr

1. Use AvtoBus Dapr transport adapter.
2. Keep Dapr components and CloudEvents externally.
3. Move C# application code to AvtoBus handlers and outbox.
4. Use Dapr as infrastructure runtime where it still adds value.

## Samples to build

### Sample 1: Modular monolith

- ASP.NET Core Orders app.
- Local commands.
- EF Core outbox.
- Domain events to local projections.
- No external broker.

### Sample 2: Microservices with RabbitMQ

- Orders API.
- Billing worker.
- Inventory worker.
- Shipping worker.
- Saga orchestration.
- Dashboard and dead-letter replay.

### Sample 3: Kafka event platform

- Order events to Kafka.
- Fraud stream processor.
- Customer activity projection.
- Replay and rebuild.

### Sample 4: Durable workflow

- Long-running order workflow.
- Activities for payment/inventory/shipping.
- Timers, signals, queries.
- Crash recovery demonstration.

### Sample 5: Interop

- AvtoBus service exchanges messages with MassTransit or NServiceBus.
- AvtoBus publishes CloudEvents consumed by Dapr app.

## Documentation plan

- Getting started in 10 minutes.
- Reliability guide: outbox, inbox, retries, dead letters.
- Routing guide by transport.
- Handler patterns and anti-patterns.
- Sagas guide.
- Durable workflows guide.
- Event sourcing and projections guide.
- Stream processing guide.
- Schema evolution guide.
- OpenTelemetry and Aspire guide.
- Migration guides for MediatR, MassTransit, NServiceBus, CAP and Dapr.
- Production checklist.

## Licensing и contribution

- Лицензия: MIT для core, чтобы снизить adoption friction. Возможна dual-license для enterprise dashboard/workflow extensions в будущем, но core должен оставаться открытым.
- Contribution model: GitHub issues + RFC discussions для API changes. Любое breaking API change проходит через ADR в `08-decisions-antipatterns-faq.md` или новый ADR doc.
- Code of Conduct и Contributing guide должны быть в репозитории с первого публичного preview.
- Samples и benchmark repository должны быть отдельными репозиториями, чтобы не загромождать core.

## v2 vision

После 1.0 stable:

- Visual workflow designer или CLI generator для saga/workflow scaffolding.
- AvtoBus Cloud или managed dashboard как optional SaaS, без lock-in для core.
- AI-assisted schema evolution и contract diff review.
- Deeper Aspire integration: auto-wiring transports, schema registry, dashboard.
- First-class F# API package.
- Embedded Temporal backend adapter и/или Azure DTFx backend adapter для workflow package.
- gRPC bidirectional streaming transport stable.
- Edge/IoT profile: NATS + MQTT + lightweight durable store.

## Hard non-goals for 1.0

- Replacing every broker-specific admin console.
- Supporting every possible transport before core reliability is stable.
- Promising exactly-once delivery across arbitrary systems.
- Building a route DSL that hides business logic.
- Requiring a sidecar or central server for simple messaging.
- Making dashboard mandatory.

## Open design questions

1. Should marker interfaces (`ICommand`, `IEvent`) be mandatory or optional with attributes/conventions?
2. Should durable workflows be built fully in AvtoBus or integrate with Temporal as a backend option?
3. Should event sourcing target PostgreSQL first or integrate EventStoreDB/Marten first?
4. How strict should Native AOT support be for 1.0?
5. Should AvtoBus enforce one command owner at startup or allow soft warnings in development?
6. Should schema registry be embedded by default or optional package?
7. Should dashboard store payload snapshots or only pointers for sensitive systems?

## Recommended first implementation path

1. Build `AvtoBus.Abstractions`, `Core`, `SourceGeneration`, `Testing`.
2. Implement in-memory transport and handler generator.
3. Implement EF Core outbox/inbox with SQLite/PostgreSQL tests.
4. Implement RabbitMQ transport.
5. Build Orders/Billing/Inventory sample and crash tests.
6. Add OpenTelemetry and health checks.
7. Add dashboard alpha and CLI route diagnostics.
8. Add PostgreSQL optimized store.
9. Add sagas.
10. Add Kafka and CloudEvents.

## Final blueprint

AvtoBus 1.0 should ship as a small reliable core plus strong adapters. The core must be fast, observable, source-generated and AOT-friendly. The winning feature is not having the most transports on day one, but making the safe architecture the easiest architecture: outbox by default, inbox by default, typed routes, schema checks, OpenTelemetry and first-class testing.
