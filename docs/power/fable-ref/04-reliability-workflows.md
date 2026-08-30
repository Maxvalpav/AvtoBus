# AvtoBus: надежность, workflows, event sourcing и streams

## Reality check: exactly-once delivery не существует в общем случае

Ни один универсальный EDA framework не должен обещать exactly-once delivery между произвольной базой, брокером и внешним API. Реалистичная цель:

- At-least-once delivery.
- Idempotent handlers.
- Inbox deduplication.
- Transactional outbox for producer correctness.
- Atomic offset/state/output only where transport and store support it, например Kafka exactly-once v2 внутри Kafka pipeline.
- Exactly-once effect for business state when inbox marker and business write are committed atomically.

## Producer correctness: outbox

Problem:

```text
Business DB write succeeds
Broker publish fails
=> downstream services never learn about state change
```

AvtoBus solution:

```text
Begin transaction
  Write business data
  Write outgoing message to avto_outbox_messages
Commit transaction
Dispatcher sends message to broker
Dispatcher marks message as dispatched
```

Outbox schema:

```sql
create table avto_outbox_messages (
    id uuid primary key,
    tenant_id text null,
    destination text not null,
    transport text not null,
    message_type text not null,
    schema_name text not null,
    schema_version int not null,
    correlation_id text null,
    causation_id text null,
    partition_key text null,
    headers jsonb not null,
    payload bytea not null,
    content_type text not null,
    created_at timestamptz not null,
    not_before timestamptz null,
    dispatched_at timestamptz null,
    attempt_count int not null default 0,
    last_error text null,
    locked_by text null,
    locked_until timestamptz null
);
```

Dispatcher requirements:

- Pull batches with `skip locked` where supported.
- Respect `not_before` for scheduled/delayed messages.
- Use exponential backoff after transient failures.
- Mark as dispatched only after transport send succeeds.
- If marking dispatched fails after send succeeds, duplicate publish can happen. This is expected and solved by inbox/idempotency.
- Emit outbox lag metric: oldest undispatched age.

## Consumer correctness: inbox и idempotency key

### Inbox (infrastructure-level dedup)

Problem:

```text
Consumer handles message
Business DB write succeeds
Ack fails or process crashes before ack
Broker redelivers message
Handler runs twice
```

AvtoBus solution:

```text
Begin transaction
  Check avto_inbox_messages by MessageId + ConsumerId
  If exists: ack and skip
  Run handler
  Write business data
  Write outgoing messages to outbox
  Insert inbox consumed marker
Commit transaction
Ack transport message
```

Inbox schema:

```sql
create table avto_inbox_messages (
    message_id uuid not null,
    consumer_id text not null,
    tenant_id text null,
    received_at timestamptz not null,
    consumed_at timestamptz not null,
    message_type text not null,
    correlation_id text null,
    primary key (message_id, consumer_id)
);
```

Cleanup:

- Retain dedupe markers by policy, e.g. 7 days for commands, 30 days for financial events.
- Use partitioned tables for high-throughput stores.
- Do not clean markers before broker retention window if replay is possible.

### Idempotency key (application-level dedup)

Idempotency key — это явный идентификатор бизнес-операции внутри payload (например, `PaymentId`, `OrderId`, `RequestId`). Если один и тот же business факт приходит через разные transport сообщения или через разные message_id (например, ретрай HTTP вызвал новый command envelope, но это та же business операция), inbox dedup не сработает, и handler должен сам определить «уже обработано».

API:

```csharp
public interface IAvtoIdempotency
{
    ValueTask<bool> TryReserveAsync(
        string scope,            // e.g. "payment", "order"
        string key,              // e.g. paymentId
        TimeSpan reservationTtl, // обычно 24-72h
        CancellationToken ct);

    ValueTask MarkCompletedAsync(
        string scope,
        string key,
        object? resultSnapshot,  // optional cache of result
        TimeSpan retention,
        CancellationToken ct);
}
```

Storage:

```sql
create table avto_idempotency (
    scope text not null,
    key text not null,
    state text not null,           -- Reserved, Completed, Failed
    result_snapshot bytea null,
    reserved_at timestamptz not null,
    expires_at timestamptz not null,
    primary key (scope, key)
);
```

Use:

```csharp
public static class CapturePaymentHandler
{
    public static async ValueTask<AvtoEffects> Handle(
        CapturePayment command,
        IAvtoIdempotency idempotency,
        AppDbContext db,
        CancellationToken ct)
    {
        if (!await idempotency.TryReserveAsync("payment", command.PaymentId, TimeSpan.FromHours(24), ct))
        {
            return AvtoEffects.None; // already in progress
        }

        try
        {
            var payment = await PaymentService.CaptureAsync(command, ct);
            db.Payments.Add(payment);

            await idempotency.MarkCompletedAsync("payment", command.PaymentId, payment, TimeSpan.FromDays(7), ct);

            return AvtoEffects.Publish(new PaymentCaptured(payment.Id));
        }
        catch
        {
            // reservation остаётся в state=Reserved до expires_at; handler должен сам решить,
            // увеличить TTL или освободить.
            throw;
        }
    }
}
```

Rules:

- Idempotency key не заменяет inbox — это второй уровень защиты.
- Idempotency key обязателен для handler, обрабатывающих финансовые/критичные события.
- TTL должен быть больше максимального времени между дублирующимися доставками.
- Failed reservation cleanup: отдельный process или retry с новым TTL.

## Retry and recoverability model

AvtoBus needs a clear failure state machine.

```text
Received
  -> Processing
  -> Succeeded -> Acked
  -> FailedTransient -> ImmediateRetry or ScheduledRetry
  -> FailedPermanent -> DeadLetter
  -> Poisoned -> Quarantine
```

Policy example:

```csharp
policies.ForAllMessages()
    .Retry(r => r.Immediate(3))
    .Then(r => r.ExponentialBackoff(5, 1.Seconds(), 1.Minutes()))
    .Then(r => r.ScheduleRetry(5.Minutes(), 30.Minutes(), 2.Hours()))
    .Then(r => r.MoveToDeadLetter());

policies.On<ValidationException>().MoveToDeadLetter("validation-error");
policies.On<UnauthorizedAccessException>().MoveToDeadLetter("security-error");
policies.On<OutOfMemoryException>().StopEndpoint();
```

Dead-letter record:

```text
DeadLetterId
OriginalMessageId
Endpoint
Reason
ExceptionType
ExceptionMessage
StackTraceHash
PayloadSnapshot or PayloadPointer
Headers
FailedAt
AttemptCount
ReplayStatus
```

Replay rules:

- Replay must create a new delivery attempt but preserve original message id for idempotency if business requires it.
- For poison payloads, replay only after code/schema fix.
- Dashboard replay actions must require authorization and audit log.

## Backpressure and flow control

AvtoBus must avoid memory overload under high load.

Controls:

- Endpoint max concurrency.
- Partition max concurrency.
- Bounded channel capacity for prefetch.
- Transport prefetch/consumer batch size.
- Outbox dispatch rate limit.
- Circuit breaker per dependency.
- Pull consumers where supported, e.g. NATS JetStream.
- Kafka pause/resume partitions.

Example:

```csharp
endpoints.Listen("orders")
    .MaxConcurrency(128)
    .Prefetch(512)
    .PartitionBy<SubmitOrder>(m => m.PartitionKey, maxParallelism: 64)
    .Backpressure(bp => bp
        .PauseWhenOutboxLagExceeds(TimeSpan.FromMinutes(5))
        .PauseWhenDependencyUnhealthy("postgres")
        .ResumeAfter(TimeSpan.FromSeconds(30)));
```

## Batch outbox dispatch

Для high-throughput систем single-message dispatch неэффективен. AvtoBus поддерживает batch mode.

### Producer side

- Handler может вернуть `AvtoEffects` с N сообщениями; они вставляются одним batch INSERT.
- Batch размер: настраивается per endpoint, default 100.
- EF Core users: `AddRange` + `SaveChangesAsync`; native providers: `COPY` (PostgreSQL) / `SqlBulkCopy` (SQL Server).
- Within business transaction: все сообщения видны диспетчеру только после commit.

### Dispatcher side

- Polling loop читает batch: `select ... from avto_outbox_messages where state = 'Pending' and next_attempt_at <= now() order by next_attempt_at limit N for update skip locked`.
- Batch size dispatcher: 100-500, настраивается.
- Parallel send внутри batch: до M concurrent send'ов в transport, при этом DB lock удерживается через `locked_until` + heartbeat.
- Per-message broker ack; при failure — release lock и retry.

### Метрики

- `avtobus_outbox_batch_size` histogram — distribution of batch sizes.
- `avtobus_outbox_dispatch_latency_seconds` histogram — from enqueue to dispatched.
- `avtobus_outbox_lock_contention_total` counter — количество раз когда row не залочен из-за skip locked.
- `avtobus_outbox_lock_held_seconds` histogram — сколько dispatcher держит lock.

## Когда Kafka idempotent producer + native transaction заменяет outbox

Важный нюанс: в Kafka с idempotent producer + exactly-once v2 producer transactions теоретически можно атомарно записать в Kafka без outbox. Но это работает только:

- Внутри Kafka pipeline.
- Producer transaction commit происходит после business write.
- Если business DB commit упал после Kafka producer commit — message отправлен без business write (zombie scenario).
- Kafka transactions не покрывают cross-system atomicity (DB + Kafka + HTTP в одной транзакции).

Поэтому рекомендация:

- Внутри Kafka-only pipeline: producer transactions + idempotent producer.
- При наличии business DB (даже если event потом в Kafka): outbox остаётся primary mechanism.
- Producer transactions в AvtoBus используются как optimization: после commit DB и outbox row insert, dispatcher может использовать transaction для batches с idempotent producer.

## Ordering and partitioning

EDA systems often need ordering per aggregate, not global ordering.

AvtoBus rule:

- Global ordering is not promised.
- Ordering can be promised per partition key when transport supports it or AvtoBus enforces single-lane local execution.
- For Kafka, partition key maps to Kafka key.
- For Azure Service Bus, partition key may map to session id.
- For RabbitMQ, use consistent-hash exchange or AvtoBus partitioned local dispatcher.

Analyzer:

- Warn if event uses ordered route without partition key.
- Warn if saga correlation key does not match route partition key.

## Sagas

Use sagas for long-running business processes where state transitions react to messages.

Saga characteristics:

- State is persisted.
- Correlation maps messages to saga instance.
- Handlers are idempotent.
- Optimistic concurrency by default.
- Timeouts are scheduled messages.
- Completion can be hard delete, soft delete or archived state.

Saga lifecycle:

```text
Start message
  -> create saga state
  -> emit commands/events/timers
Follow-up messages
  -> load saga by correlation
  -> update state
  -> emit commands/events/timers
Complete
  -> mark completed
  -> cancel timers if needed
```

Concurrency policy:

- Optimistic concurrency with version column.
- On conflict, retry message with short jitter.
- For hot saga keys, single-flight lock or partitioned endpoint.

Compensation:

- Prefer forward recovery over rollback.
- Use explicit compensation commands, e.g. `RefundPayment`, `ReleaseInventory`.
- Every compensation command must be idempotent.

## Durable workflows

Use durable workflows when a process is code-shaped, long-running, needs durable timers, signals, queries and crash recovery by replay.

Workflow history events:

```text
WorkflowStarted
ActivityScheduled
ActivityCompleted
ActivityFailed
TimerScheduled
TimerFired
SignalReceived
WorkflowDecisionRecorded
WorkflowCompleted
WorkflowFailed
WorkflowContinuedAsNew
```

Workflow execution model:

- Workflow code runs deterministically.
- External side effects happen only in activities.
- Workflow state is reconstructed by replaying history.
- Timers are durable scheduled messages, not `Task.Delay`.
- Signals mutate workflow state through recorded history.
- Queries read current workflow state without mutation.

Analyzer restrictions:

| Forbidden in workflow | Use instead |
| --- | --- |
| `DateTime.UtcNow` | `context.Now` |
| `Guid.NewGuid()` | `context.NewGuid()` recorded in history |
| `Random.Shared` | deterministic workflow random from context if needed |
| `Task.Delay` | `context.CreateTimer` |
| HTTP/DB/file I/O | activity |
| `Task.Run` | workflow scheduler APIs |

Durable workflow storage:

```sql
create table avto_workflow_instances (
    id text primary key,
    workflow_type text not null,
    status text not null,
    version bigint not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    completed_at timestamptz null,
    state_snapshot bytea null
);

create table avto_workflow_history (
    workflow_id text not null,
    sequence bigint not null,
    event_type text not null,
    payload bytea not null,
    created_at timestamptz not null,
    primary key (workflow_id, sequence)
);
```

## Event sourcing

Event sourcing should be optional, but deeply integrated.

Use cases:

- Audit-critical domains.
- Complex aggregates where history matters.
- Rebuildable projections.
- Temporal queries and debugging.

Core abstractions:

```csharp
public interface IAvtoEventStore
{
    ValueTask AppendAsync(
        string streamName,
        long expectedVersion,
        IReadOnlyList<IEvent> events,
        CancellationToken ct);

    IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamName,
        long fromVersion,
        CancellationToken ct);
}
```

Append rules:

- Use expected version for optimistic concurrency.
- Write integration outbox messages in the same transaction if events need external publication.
- Store metadata: correlation id, causation id, user id, tenant id, trace id.

Snapshots:

- Optional per aggregate.
- Snapshot after N events or size threshold.
- Snapshot schema versioned.

Upcasting:

- Every event version can be upcast to current in-memory contract.
- Upcasters must be deterministic and tested.

## Projections

Projection types:

- Live projection: updates inside command transaction for strong local reads.
- Async projection: updates from event stream or broker.
- Rebuildable projection: can be dropped and rebuilt from event store.
- External projection: pushes into Elasticsearch, Redis, ClickHouse, lakehouse.

Projection checkpoint:

```sql
create table avto_projection_checkpoints (
    projection_name text not null,
    shard text not null,
    position text not null,
    updated_at timestamptz not null,
    primary key (projection_name, shard)
);
```

Projection guarantees:

- At-least-once event delivery.
- Deduplicate by event id.
- Checkpoint after transaction commits.
- Support rebuild with isolated target table and atomic swap.

## Stream processing

AvtoBus Streams should bring Kafka Streams ideas to .NET, while staying broker-capability aware.

Primitives:

- Source topic/stream.
- Key by partition key.
- Map/filter/flatMap.
- Window: tumbling, hopping, sliding, session.
- Grace period for out-of-order events.
- Aggregate with state store.
- Join stream-stream and stream-table where supported.
- Sink to event topic or projection store.
- Interactive queries for state stores.

State stores:

- In-memory for tests.
- RocksDB optional native package.
- PostgreSQL/SQL Server for simpler operations.
- Kafka compacted changelog for Kafka mode.

Processing guarantees:

- Kafka mode can use producer transactions and exactly-once v2 semantics for input offsets, state and output topics.
- Non-Kafka mode uses inbox/outbox and idempotent state updates.

## Schema evolution

Compatibility modes:

- None: no compatibility checks.
- Backward: new consumers can read old messages.
- Forward: old consumers can read new messages.
- Full: both backward and forward.
- Strict: no breaking changes and required approval for additions.

Rules:

- Additive nullable fields are usually safe.
- Removing fields is breaking unless ignored by all consumers.
- Changing type is breaking.
- Renaming is remove + add unless explicit alias is provided.
- Changing semantic meaning is breaking even if schema shape stays valid.

Schema outputs:

- JSON Schema.
- AsyncAPI.
- Protobuf descriptors if protobuf package is used.
- Markdown contract docs.

## Interop

AvtoBus should support gradual migration.

Interop modes:

- CloudEvents binary/structured mode.
- MassTransit envelope adapter.
- NServiceBus headers adapter.
- Dapr Pub/Sub adapter.
- Raw Kafka JSON/Protobuf messages.
- Debezium CDC input adapter.
- Webhook input/output adapter.

Interop principle:

- Interop adapters should be endpoint-specific.
- Native AvtoBus envelope remains internal canonical model.
- Header mapping must be visible in diagnostics.

## Operations checklist

Production app should be able to answer:

- Which routes own each command?
- Which services subscribe to each event?
- What is current outbox lag?
- Which messages are in dead-letter and why?
- Which workflows are stuck and on what timer/activity?
- What is consumer lag by endpoint and partition?
- Which schema versions are active in production?
- Can a projection be rebuilt safely?
- Can a failed message be replayed with audit trail?

AvtoBus must make these answers available through dashboard, CLI and metrics.
