# AvtoBus: observability semantic conventions, chaos и backfill

Документ детализирует три области, которые в предыдущих файлах описаны на уровне принципов: telemetry semantic conventions, chaos/failure injection и backfill новых consumers.

## OpenTelemetry semantic conventions для messaging

AvtoBus следует [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/) там, где это применимо, и расширяет через `avtobus.*` namespace для своих абстракций.

### Обязательные атрибуты

| Атрибут | Где | Значение |
| --- | --- | --- |
| `messaging.system` | span, log | `rabbitmq`, `kafka`, `nats`, `azure_servicebus`, `aws_sqs`, `redis`, `dapr`, `inmemory`, `avtobus.local` |
| `messaging.operation` | span | `publish`, `send`, `receive`, `process` |
| `messaging.destination.name` | span | routing key / topic / queue name |
| `messaging.destination.kind` | span | `queue`, `topic` |
| `messaging.message.id` | span | `envelope.message_id` |
| `messaging.message.conversation_id` | span | `envelope.conversation_id` |
| `messaging.message.body.size` | span | размер payload bytes |
| `messaging.batch.message_count` | span, при batch | сколько сообщений в batch |

### AvtoBus extensions

| Атрибут | Назначение |
| --- | --- |
| `avtobus.message.type` | concrete type name |
| `avtobus.schema.name` | schema name |
| `avtobus.schema.version` | schema version int |
| `avtobus.tenant.id` | tenant id |
| `avtobus.correlation.id` | business correlation id |
| `avtobus.causation.id` | parent message id |
| `avtobus.partition.key` | partition key |
| `avtobus.consumer.id` | receiver name (queue) |
| `avtobus.handler.name` | handler method name |
| `avtobus.outbox.attempt` | attempt number |
| `avtobus.outbox.state` | Pending/Dispatching/Dispatched/Failed |
| `avtobus.workflow.id` | workflow instance id |
| `avtobus.workflow.sequence` | history sequence number |
| `avtobus.saga.id` | saga id |
| `avtobus.saga.version` | saga version |
| `avtobus.projection.name` | projection name |
| `avtobus.projection.shard` | shard id |

### Span kind

- Producer (publish/send): `client` или `producer` (OpenTelemetry messaging conventions).
- Consumer (receive): `consumer` (с `messaging.operation=process` для handler span внутри).

### Span tree для одного consumer event

```text
transport.receive (consumer, root)
├── inbox.check (internal)
├── handler (internal)
│   ├── validator (internal)
│   ├── loader (internal)
│   ├── business (internal)
│   │   ├── db.write (client, db.system=postgresql)
│   │   └── db.write outbox (client, db.system=postgresql)
│   └── effect.materialize (internal)
├── outbox.save (client, db.system=postgresql)
└── inbox.mark (client, db.system=postgresql)
```

Каждый span имеет одинаковый `TraceId` и парентится по `SpanId`. Это даёт в trace UI полный контекст.

### W3C trace context

Traceparent и tracestate пробрасываются через envelope headers и через transport-specific location:

| Transport | Где пробрасывается |
| --- | --- |
| RabbitMQ | headers `traceparent`, `tracestate` |
| Kafka | record headers (а не key/value) |
| Azure Service Bus | `ApplicationProperties` |
| NATS | subject headers / message headers |
| HTTP | `traceparent` HTTP header |
| gRPC | `grpc-trace-bin` metadata |
| In-process | `Activity.Current` |

Failure mode при broken propagation:

- Если envelope не имеет `traceparent`, создаётся новый trace с `avtobus.broken_propagation = true` attribute.
- Метрика `avtobus_trace_propagation_failures_total` инкрементируется.
- Alert на эту метрику — индикатор, что клиент или transport обрезал headers.

### Clock skew

- `created_at` и `not_before` хранятся в UTC.
- При durable timers AvtoBus нормализует server clock skew через logical clock: вместо абсолютного `scheduled_at - now()` используется `next_attempt_at = max(now() + delay, current_server_now + safety_window)`.
- Если `now() > expires_at` на receive — envelope помечается `messaging.message.dropped = expired` и не обрабатывается.
- `safety_window` (default 1s) предотвращает ранние fires из-за мелких skew.

## Failure injection (chaos testing)

Chaos testing matrix должен покрывать основные failure modes до того, как они встретятся в production.

### Категории failures

#### Transport failures

| Failure | Как воспроизводить | Ожидаемый результат |
| --- | --- | --- |
| Broker network partition | iptables drop + chaos-mesh | outbox lag растёт, no message loss |
| Broker slow | toxiproxy delay | consumer lag растёт, retry counter инкрементируется |
| Broker connection refused | point at wrong host | health check Unhealthy, dispatcher backoff |
| Consumer connection drop mid-message | kill connection | redelivery, inbox dedup работает |
| Lost ack | chaos intercept ack | redelivery, handler idempotent |

#### Store failures

| Failure | Ожидаемый результат |
| --- | --- |
| Postgres primary failover | dispatcher reconnect, outbox re-reads, no message loss |
| Postgres read replica lag | projection checkpoint не впереди store; event sequence не сломан |
| Outbox table locked (long business transaction) | dispatcher ждёт, lag растёт, health Degraded |
| EF Core migration during traffic | миграция должна быть online-совместимой; если нет — maintenance window |
| Connection pool exhaustion | handlers timeout, retries, no DB corruption |

#### Process failures

| Failure | Ожидаемый результат |
| --- | --- |
| SIGKILL после DB commit, до broker send | outbox dispatch после restart |
| SIGKILL во время handler | redelivery + inbox dedup или saga version conflict retry |
| SIGKILL во время workflow history append | workflow instance Faulted, manual recovery через history |
| OOM kill под нагрузкой | backpressure, drop prefetch, graceful shutdown |
| Pod evicted by K8s | drain handler active, replicas balance |
| Network blip между app и broker | reconnect с jitter, retries |

#### App-level failures

| Failure | Ожидаемый результат |
| --- | --- |
| Handler throws | retry per policy, затем DLQ |
| Validator fails | DLQ immediately, reason=validation |
| Saga conflict | retry with jitter, after N → DLQ |
| Workflow non-deterministic API call | analyzer catches before deploy; runtime защита через replay + state diff |
| Schema mismatch | consumer logs incompatible, drops или DLQ по policy |

### Инструменты

- **chaos-mesh** / **Gremlin** / **Litmus** — K8s network chaos.
- **toxiproxy** — TCP proxy с latency, drop, slow-close.
- **Wireshark/tcpdump** — для network trace.
- **dotnet avto chaos** — встроенный fault injector для dev/test, например:
  - `dotnet avto chaos inject --endpoint orders --failure outbox-dispatch-delay --duration 30s`
  - `dotnet avto chaos inject --handler SubmitOrderHandler --failure throw-on-attempt 2`

### Тестовые сценарии

- `CrashAfterCommitBeforeBrokerSend` — kill -9 в момент между DB commit и broker send.
- `BrokerUnavailableExtended` — broker offline 10 минут, проверка, что после восстановления outbox догоняет.
- `ConsumerCrashMidBatch` — kill в середине batch, проверка redelivery + inbox.
- `SagaConcurrentMessages` — две одновременные message с одним correlation, проверка optimistic concurrency.
- `WorkflowReplayConsistency` — re-execute workflow от старого snapshot, проверка детерминированности.
- `OutboxDispatcherFailure` — dispatcher throws на половине batch, проверка lock release.
- `SchemaEvolutionBackward` — old consumer читает new message, проверка optional fields.

Каждый сценарий должен:

1. Создавать baseline metrics.
2. Запустить fault.
3. Наблюдать поведение.
4. Восстановить систему.
5. Подтвердить, что состояние converged.

## Backfill: новый consumer получает историю

Когда новый сервис подписывается на события, у которых уже есть история, ему нужна начальная позиция. AvtoBus даёт несколько стратегий.

### Start position strategies

| Strategy | Описание |
| --- | --- |
| `from-now` | только новые события, история пропускается |
| `from-beginning` | читать всю историю с первого события в stream |
| `from-offset` | читать с указанного offset/sequence (Kafka, NATS) |
| `from-timestamp` | читать начиная с UTC timestamp (event time) |
| `from-snapshot` | загрузить snapshot projection и replay events с snapshot position |

### Backfill workflow

```text
1. Operator регистрирует новый endpoint
2. AvtoBus вычисляет source positions per shard
3. Source: event store (preferred, deterministic) или broker topic
4. Replay worker читает batch (size = backfill.batch.size)
5. Для каждого batch:
   a. Deserialize + upcast если schema_version < current
   b. Валидация
   c. Invoke handler через normal pipeline (с outbox/inbox, как production)
   d. Checkpoint update
6. Rate-limit: max X messages/sec (per consumer config)
7. Progress metric: avtobus_backfill_progress_percent
8. Resume: после рестарта продолжает с последнего checkpoint
```

### Safety rules

- Backfill НЕЛЬЗЯ нарушать ordering per partition key.
- Backfill НЕ должен влиять на production throughput. Выделенный backfill pool или limited concurrency.
- Если backfill пишет в ту же projection что и live consumer — projection должен поддерживать split: live shards (recent) и backfill shards (historical), merge по checkpoint.
- Если projection rebuild — двойная запись: shadow projection + atomic swap при достижении live position.

### CLI

```bash
dotnet avto backfill plan --consumer order-search --from-timestamp 2026-01-01T00:00:00Z
dotnet avto backfill run --consumer order-search --rate 1000 --concurrency 8
dotnet avto backfill status --consumer order-search
dotnet avto backfill pause --consumer order-search
dotnet avto backfill resume --consumer order-search
dotnet avto backfill cancel --consumer order-search
```

## Replay of dead-letter

```bash
dotnet avto deadletter list --endpoint orders --since 24h
dotnet avto deadletter inspect --id 01JZ...
dotnet avto deadletter replay --id 01JZ... --reason "fix in PR #1234" --authorized-by ops@company.com
dotnet avto deadletter replay-bulk --query "endpoint=orders AND reason=validation AND age>1h"
```

Replay всегда:

- Требует authorization role.
- Пишет audit log entry с user, timestamp, reason.
- Использует original `message_id` если только business logic не требует новый id.
- Помечает replayed_at на row в `avto_dead_letters`.
- Поддерживает dry-run mode для pre-check схемы/совместимости.

## Health probe composition

```text
Healthy = All Critical probes Healthy
        AND no Warning probes Unhealthy for >N minutes

Critical:
  - avtobus-transport-{transport} (must be Healthy)
  - avtobus-outbox-{store} (must be Healthy)
  - avtobus-inbox-{store} (must be Healthy)

Warning:
  - avtobus-scheduler (Degraded если lag > 60s)
  - avtobus-projection-{name} (Degraded если lag > 300s)
  - avtobus-schema-registry (Degraded если unreachable)
```

KEDA scaler должен смотреть на `avtobus_endpoint_queue_depth` и `avtobus_endpoint_consumer_lag`. Health endpoint может использоваться для zero-replica decisions.

## Live observability dashboard minimums

Operations dashboard должен показывать в real time:

1. Throughput by message_type и transport.
2. p50/p95/p99 latency by message_type.
3. Error rate by exception_type.
4. Outbox lag p99 и oldest pending age.
5. Inbox dedupe rate (как % от processed).
6. Dead letter rate, top reasons.
7. Endpoint consumer lag by partition.
8. Saga count by status.
9. Workflow count by status.
10. Projection lag by name и shard.
11. Schema compatibility status.
12. Active chaos injections (если dev).
13. KEDA scaling status.
