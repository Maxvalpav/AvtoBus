# AvtoBus: архитектура фреймворка

## Архитектурная цель

AvtoBus должен стать application framework для event-driven .NET-систем, а не thin wrapper над брокерами. Он работает в ASP.NET Core 10/11, Worker Service, Aspire AppHost, Kubernetes, serverless containers и modular monolith.

Главная идея: один message kernel, несколько execution modes.

```text
ASP.NET Core endpoint
        |
        v
Application service / Minimal API
        |
        v
IAvtoBus.InvokeAsync / SendAsync / PublishAsync / ScheduleAsync
        |
        v
Generated message pipeline
        |
        +--> local in-process queue
        +--> transactional outbox
        +--> broker transport
        +--> workflow/event-store/stream processor
```

## Core layers

### 1. Contracts layer

Contracts are messages that cross boundaries.

```csharp
public interface IAvtoMessage
{
    static abstract string SchemaName { get; }
    static abstract int SchemaVersion { get; }
}

public interface ICommand : IAvtoMessage;
public interface ICommand<out TReply> : ICommand;
public interface IEvent : IAvtoMessage;
public interface IQuery<out TReply> : IAvtoMessage;

public interface IPartitionedMessage
{
    string PartitionKey { get; }
}
```

Design decision:

- Marker interfaces are allowed for developer clarity.
- Routing and schema identity are based on concrete type, not interface dispatch.
- Every integration message must have schema name, version and compatibility policy.

### 2. Envelope layer

Every message gets a standard envelope.

```text
AvtoEnvelope
  MessageId
  MessageType
  SchemaName
  SchemaVersion
  CorrelationId
  CausationId
  ConversationId
  TraceParent
  TraceState
  TenantId
  PartitionKey
  CreatedAt
  NotBefore
  ExpiresAt
  ContentType
  Headers
  Payload
```

CloudEvents mapping:

| AvtoEnvelope | CloudEvents |
| --- | --- |
| MessageId | id |
| MessageType/SchemaName | type |
| Source | source |
| CreatedAt | time |
| ContentType | datacontenttype |
| SchemaUri | dataschema |
| CorrelationId | extension |
| TraceParent | W3C traceparent extension |

### 3. Source-generated execution layer

AvtoBus generator scans handlers at compile time and creates pipeline code.

Handler example:

```csharp
public static class SubmitOrderHandler
{
    public static async ValueTask<(OrderAccepted Reply, OrderSubmitted Event)> Handle(
        SubmitOrder command,
        AppDbContext db,
        TimeProvider clock,
        CancellationToken ct)
    {
        var order = Order.Create(command.OrderId, command.CustomerId, clock.GetUtcNow());
        db.Orders.Add(order);

        return (
            new OrderAccepted(command.OrderId),
            new OrderSubmitted(command.OrderId, command.CustomerId, order.CreatedAt));
    }
}
```

Generated pipeline responsibilities:

- Deserialize envelope payload using generated serializers.
- Resolve dependencies by method parameter.
- Run validation, authorization and tenant middleware only where relevant.
- Open outbox/inbox transaction when needed.
- Invoke handler without reflection.
- Convert return values to explicit effects.
- Save outgoing messages to outbox.
- Ack/nack or commit offsets based on transaction result.
- Record OpenTelemetry spans and metrics.

### 4. Durability layer

Durability is transport-independent и описан в деталях в [09-durability-store-contract.md](09-durability-store-contract.md). Этот раздел — обзор.

Stores:

- Outbox store (состояния Pending/Dispatching/Dispatched/Failed, distributed lock через skip locked).
- Inbox/deduplication store (PRIMARY KEY по `message_id + consumer_id`).
- Saga store (optimistic concurrency через `version`).
- Workflow store (history append-only + snapshot).
- Scheduled message store (claim + dispatch).
- Dead-letter store (replay audit + optional payload snapshot).
- Event store (append-only по stream/version, expected version check).
- Projection checkpoint store.
- Envelope store (общий body для всех сообщений, чтобы не дублировать payload).
- Schema registry store (version, compatibility status, payload hash).

Supported persistence adapters:

- EF Core (generic, recommended для simple cases).
- PostgreSQL native (best support: skip locked, jsonb, advisory locks, partial indexes).
- SQL Server native (readpast + updlock + version-based concurrency).
- MongoDB (preview).
- Cosmos DB (preview).
- Marten/EventStoreDB optional event sourcing adapters (для тех, кто хочет external event store).

Core tables for relational stores:

```text
avto_envelopes                    -- shared body и metadata
avto_outbox_messages              -- producer reliability
avto_inbox_messages               -- consumer dedup
avto_scheduled_messages           -- delayed delivery
avto_dead_letters                 -- terminal failure records
avto_sagas                        -- long-running state
avto_workflow_instances           -- workflow metadata
avto_workflow_history             -- append-only history
avto_workflow_timers              -- durable timers
avto_event_streams                -- event store append-only
avto_projection_checkpoints       -- per-shard cursor
avto_schema_registry              -- schemas, versions, compatibility
avto_store_version                -- store schema version
```

### 5. Transport layer

Transport adapters implement a capability-based interface.

```csharp
public interface IAvtoTransport
{
    string Name { get; }
    AvtoTransportCapabilities Capabilities { get; }

    ValueTask SendAsync(AvtoOutgoingEnvelope envelope, CancellationToken ct);
    IAsyncEnumerable<AvtoIncomingEnvelope> ReceiveAsync(AvtoReceiveEndpoint endpoint, CancellationToken ct);
}
```

Capabilities:

```csharp
[Flags]
public enum AvtoTransportCapabilities
{
    None = 0,
    Queues = 1,
    Topics = 2,
    ConsumerGroups = 4,
    PartitionOrdering = 8,
    DelayedDelivery = 16,
    NativeDeadLetter = 32,
    Transactions = 64,
    OffsetCommit = 128,
    Replay = 256,
    Compaction = 512,
    Sessions = 1024,
    PullConsumers = 2048,
    CloudEventsNative = 4096
}
```

Why capabilities matter:

- Kafka supports replay, partitions and offset commits, but not command queues in the same way as RabbitMQ.
- RabbitMQ supports rich routing and queues, but not stream-table processing as a first-class concept.
- Azure Service Bus sessions are useful for saga/entity affinity.
- NATS JetStream pull consumers give direct backpressure control.

### 6. Policy layer

Policies are composable and filterable per message type, endpoint, exception type, tenant or transport.

Policy categories:

- Retry and redelivery.
- Timeout and cancellation.
- Circuit breaker and rate limiting.
- Concurrency and partitioning.
- Transaction and outbox mode.
- Serialization and schema compatibility.
- Security and PII masking.
- Dead-letter and quarantine.
- Observability sampling.

Example:

```csharp
bus.Policies(policy => policy
    .ForMessagesInNamespace("Orders.Contracts")
    .UseOutbox()
    .Retry(r => r.ExponentialBackoff(maxAttempts: 5, minDelay: 100.Milliseconds(), maxDelay: 30.Seconds()))
    .On<ValidationException>().MoveToDeadLetter(reason: "validation")
    .On<SqlException>().ScheduleRetry(1.Minutes(), 5.Minutes(), 30.Minutes())
    .Concurrency(c => c.PartitionByMessageKey(maxParallelism: 64)));
```

### 7. Observability layer

AvtoBus must emit telemetry by default:

- OpenTelemetry traces around send, publish, receive, handler, outbox save, outbox dispatch, workflow step and projection batch.
- OpenTelemetry metrics: throughput, latency, retries, dead letters, outbox lag, consumer lag, handler failure count, processing duration, queue depth if transport supports it.
- Structured logs with message id, correlation id, causation id, tenant id, handler name and endpoint.
- Health checks for transports, outbox lag, schema registry, projection lag and dead-letter growth.
- Aspire integration: dashboard-friendly OTLP, service discovery and resource binding.

.NET 11 direction matters: .NET 11 preview includes runtime-native async, System.Text.Json improvements, built-in OpenTelemetry metrics in libraries and ASP.NET Core native OTel semantic attributes. AvtoBus should align with ActivitySource, Meter, ILogger and avoid vendor-specific telemetry APIs.

## Runtime flows

### Send command with transactional outbox

```text
HTTP POST /orders
  -> Minimal API validates request
  -> db transaction begins
  -> business state changes
  -> AvtoBus Send/Publish stores outgoing envelopes in avto_outbox_messages
  -> db transaction commits
  -> outbox dispatcher sends to broker
  -> send status marked dispatched
```

Guarantee:

- No zombie record: business data is not committed without the outgoing envelope record.
- Broker send may happen more than once if marking dispatched fails, so consumers still need inbox/idempotency.

### Receive with inbox and handler transaction

```text
Transport receives envelope
  -> parse headers and start consumer span
  -> check inbox by MessageId + ConsumerId
  -> if duplicate: ack and skip
  -> begin business transaction
  -> run generated handler pipeline
  -> write business changes
  -> write outgoing messages to outbox
  -> write inbox consumed marker
  -> commit
  -> ack/commit offset
```

Guarantee:

- At-least-once delivery from broker.
- Exactly-once effect when handler state and inbox marker are committed atomically.

### Workflow execution

```text
Command/event starts workflow
  -> workflow instance created
  -> workflow history appends StepScheduled
  -> activity command emitted through outbox
  -> activity result event returns
  -> workflow rehydrates from history
  -> next deterministic decision is made
```

Workflow modes:

- Saga mode: simpler state machine, no deterministic replay requirement.
- Durable workflow mode: Temporal-like event history replay, durable timers, signals, queries and activities.

## ASP.NET Core 10/11 integration

### Hosting

AvtoBus runs as hosted services:

- `AvtoReceiverHostedService` per receive endpoint group.
- `AvtoOutboxDispatcherHostedService` per outbox store.
- `AvtoSchedulerHostedService` for due scheduled messages.
- `AvtoProjectionHostedService` for projections.
- `AvtoWorkflowWorkerHostedService` for workflows and activities.

### DI and options

Use idiomatic .NET patterns:

- `IServiceCollection.AddAvtoBus(...)`.
- `IHostApplicationBuilder.AddAvtoBus(...)` for Aspire-friendly setup.
- `IOptions<AvtoBusOptions>` and named transport options.
- `IHealthChecksBuilder.AddAvtoBus()`.
- `IEndpointRouteBuilder.MapAvtoBusDashboard()` for optional dashboard.

### Minimal API helpers

```csharp
app.MapPost("/orders", async (SubmitOrder command, IAvtoBus bus, CancellationToken ct) =>
{
    var result = await bus.InvokeAsync<OrderAccepted>(command, ct);
    return Results.Accepted($"/orders/{result.OrderId}", result);
});
```

### Native AOT and trimming

AvtoBus must be designed for AOT-readiness:

- Source-generated serializers.
- Source-generated handler registry.
- Avoid dynamic proxy generation in core.
- Reflection only behind explicit opt-in packages.
- Analyzer warnings for unsupported patterns.

### Runtime Async and ValueTask

.NET 11 runtime-native async reduces overhead and improves stack traces. AvtoBus should still expose `ValueTask` where hot paths benefit:

- Handler invocation.
- Serialization/deserialization.
- Transport send/receive.
- Inbox/outbox store operations.

## Schema versioning и codec negotiation

Envelope хранит `schema_name` и `schema_version`. Диспетчер и потребитель должны договориться о codec.

Codec resolution на стороне producer:

```text
outbox row -> schema_registry.lookup(name, version) -> codec_id
if codec_id == null: write failure (schema not registered)
if compatibility_status != Active: write failure (cannot produce)
```

Codec resolution на стороне consumer:

```text
envelope -> schema_registry.lookup(name, envelope.version)
if not found and policy = drop: increment dropped
if not found and policy = dlq: route to dead-letter
if codec = current: invoke handler с current type
if codec < current: apply upcasters chained to current
if codec > current: apply downcaster или reject (depends on compatibility mode)
```

Upcasters:

- Pure functions: `T_old -> T_new`.
- Зарегистрированы per schema name и version range.
- Диспетчер upcast'ит до текущей версии при чтении из event store.
- Upcasters обязаны быть детерминированными и идемпотентными.

Downcasters не приветствуются: вместо этого новые поля при чтении старым consumer'ом должны отсутствовать. Если это невозможно — breaking change, требующий versioned consumer deployment.

## AvtoBus версионирование и SemVer

- Сам AvtoBus следует SemVer 2.0.0.
- Major bump: breaking API change в `AvtoBus.Abstractions` или `AvtoBus.Core`.
- Minor bump: backwards-compatible new API, новые packages, новые transports.
- Patch bump: bug fix, без API изменений.
- Preview packages: 0.x.y. SemVer не гарантируется; стабилизация при переходе к 1.0.

Compatibility matrix AvtoBus:

| AvtoBus major | .NET LTS | .NET current | ASP.NET Core |
| --- | --- | --- | --- |
| 1.x | .NET 10 | .NET 11 preview | ASP.NET Core 10/11 |
| 0.x (preview) | .NET 10 | .NET 11 preview | ASP.NET Core 10/11 |

Поддержка preview-версий .NET: пока они поддерживаются Microsoft, AvtoBus их поддерживает и тестирует. Если Microsoft делает release breaking change, AvtoBus может временно дропнуть preview до исправления.

Native AOT readiness matrix (1.0 goal):

| Package | AOT-ready | Notes |
| --- | --- | --- |
| AvtoBus.Abstractions | yes | только contracts |
| AvtoBus.Core | yes | source-generated pipeline |
| AvtoBus.Hosting.AspNetCore | yes | DI wiring source-generated |
| AvtoBus.SourceGeneration | yes | compile-time, не runtime |
| AvtoBus.Serialization.SystemTextJson | yes | source-generated |
| AvtoBus.Durability.EFCore | partial | EF Core trimmed mode support |
| AvtoBus.Durability.PostgreSql | yes | Npgsql + source-gen |
| AvtoBus.Durability.SqlServer | yes | Microsoft.Data.SqlClient |
| AvtoBus.Transport.RabbitMQ | yes | RabbitMQ.Client 7+ |
| AvtoBus.Transport.Kafka | yes | Confluent.Kafka с reflection trim warnings |
| AvtoBus.Transport.Nats | yes | NATS.NET |
| AvtoBus.Dashboard | no | Blazor server, runtime reflection |
| AvtoBus.Testing | no | test-only |

## Package architecture

```text
AvtoBus.Abstractions
AvtoBus.Core
AvtoBus.Hosting.AspNetCore
AvtoBus.SourceGeneration
AvtoBus.Analyzers
AvtoBus.Serialization.SystemTextJson
AvtoBus.Serialization.Protobuf
AvtoBus.Durability.EFCore
AvtoBus.Durability.PostgreSql
AvtoBus.Durability.SqlServer
AvtoBus.Transport.RabbitMQ
AvtoBus.Transport.Kafka
AvtoBus.Transport.Nats
AvtoBus.Transport.AzureServiceBus
AvtoBus.Transport.Aws
AvtoBus.Transport.RedisStreams
AvtoBus.Transport.Dapr
AvtoBus.Workflow
AvtoBus.EventSourcing
AvtoBus.Streams
AvtoBus.Testing
AvtoBus.Aspire
AvtoBus.Dashboard
AvtoBus.Cli
```

## Internal object model

```text
AvtoBusRuntime
  HandlerGraph
  RouteTable
  EndpointGraph
  PolicyGraph
  SerializerRegistry
  SchemaRegistry
  TransportRegistry
  DurabilityStoreRegistry
  Telemetry
```

HandlerGraph:

- Built at compile time by generator.
- Validated at startup.
- Supports hot diagnostics: print route map, missing handler map, conflicting subscriptions.

RouteTable:

- Commands: exactly one logical owner unless explicitly configured as fanout command.
- Events: zero or more subscribers.
- Queries: one handler, local or remote request-response.

EndpointGraph:

- Receive endpoints.
- Send endpoints.
- Subscriptions.
- Consumer groups.
- Partitions.
- Dead-letter endpoints.

PolicyGraph:

- Ordered policy evaluation.
- More specific policies override broad policies.
- Analyzer warns about conflicting policies.

## Security model

Security should be built into envelope and pipeline:

- Tenant isolation via tenant id and tenant resolver.
- Message signing and optional encryption.
- PII masking in logs and dead-letter payloads.
- Authorization policy per command/event/query.
- Schema allowlist per endpoint.
- Max message size and max header size.
- Poison message quarantine after repeated failures.
- Replay protection for externally received messages.

## Dashboard architecture

Dashboard must be optional and production-safe.

Views:

- Live throughput and failure rate.
- Outbox lag and dispatcher health.
- Inbox dedupe statistics.
- Endpoint map and route graph.
- Dead letters with safe payload inspection.
- Retry and replay actions with authorization.
- Workflow instances and timers.
- Projection lag and rebuild status.
- Schema versions and compatibility status.

## Key architectural decision

AvtoBus core should not depend on any transport, database, dashboard or workflow package. Core owns envelope, routing, generated handler pipeline, policy model and telemetry abstractions. Everything else is adapter/plugin.
