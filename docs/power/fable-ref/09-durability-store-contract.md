# AvtoBus: durability store contract

Цель этого документа — зафиксировать контракт durability store так, чтобы любой backend (PostgreSQL, SQL Server, MongoDB, Cosmos, Redis, pluggable) реализовывал его одинаково, а разработчик мог полагаться на поведение.

## Сквозные инварианты

1. Atomic write: бизнес-данные и outbox сообщения коммитятся в одной транзакции. После commit сообщения видны диспетчеру.
2. At-least-once dispatch: диспетчер может отправить сообщение брокеру более одного раза. Потребитель обязан быть идемпотентным или использовать inbox.
3. Inbox dedupe: один и тот же `MessageId + ConsumerId` обрабатывается ровно один раз.
4. Optimistic concurrency: любое обновление saga/workflow состояния проверяет `expected_version`. На конфликте — retry с jitter.
5. Append-only history: workflow history и event store никогда не перезаписываются. Snapshot — отдельная сущность.
6. Checkpoint atomic: projection checkpoint и outbox-вычитка/коммит происходят атомарно.
7. Tenant isolation: tenant_id прокидывается во все таблицы; для строгой изоляции — отдельная база per tenant (multi-store).

## Общий envelope envelope rows

```sql
-- Ядро envelope, используется во всех таблицах сообщений и event store
create table avto_envelopes (
    id uuid not null,
    tenant_id text null,
    message_id uuid not null,
    message_type text not null,
    schema_name text not null,
    schema_version int not null,
    correlation_id text null,
    causation_id text null,
    conversation_id text null,
    partition_key text null,
    trace_parent text null,
    trace_state text null,
    headers jsonb not null,
    content_type text not null,
    payload bytea not null,
    created_at timestamptz not null,
    not_before timestamptz null,
    expires_at timestamptz null,
    primary key (id)
);

create unique index ux_avto_envelopes_message_id
    on avto_envelopes (message_id, tenant_id);
```

Сущности используют `envelope_id` (FK на `avto_envelopes.id`) для payload-общего тела. Это избегает дублирования payload во всех таблицах.

## Outbox

```sql
create table avto_outbox_messages (
    id uuid primary key,
    envelope_id uuid not null references avto_envelopes(id) on delete cascade,
    destination text not null,
    transport text not null,
    attempt_count int not null default 0,
    max_attempts int not null default 0,
    next_attempt_at timestamptz not null,
    locked_by text null,
    locked_until timestamptz null,
    state text not null,           -- Pending, Dispatching, Dispatched, Failed
    created_at timestamptz not null,
    dispatched_at timestamptz null,
    last_error text null,
    last_error_class text null,
    dead_letter_id uuid null,
    constraint fk_outbox_envelope foreign key (envelope_id) references avto_envelopes(id)
);

create index ix_outbox_pending
    on avto_outbox_messages (next_attempt_at)
    where state in ('Pending', 'Dispatching');

create index ix_outbox_tenant_pending
    on avto_outbox_messages (envelope_id)
    where state in ('Pending', 'Dispatching');
```

### Состояния

| State | Описание |
| --- | --- |
| `Pending` | ожидает диспетчеризации |
| `Dispatching` | залочено воркером, отправляется |
| `Dispatched` | доставлено в transport |
| `Failed` | превышены attempts, перемещено в dead-letter |

### Защита от race

- `select ... for update skip locked` (PostgreSQL) или `with (readpast, updlock)` (SQL Server).
- Lock TTL: `locked_until` обновляется периодически. По истечении — row считается украденной.
- Multi-dispatcher: безопасны за счёт skip locked.
- Idempotency: `message_id` уникален. Дубликаты вставляются как `Dispatched` (если вставка из retry-loop).

### Batch insert

Рекомендуется вставлять через `COPY` (PostgreSQL) или `SqlBulkCopy` (SQL Server). EF Core users могут использовать `EFCore.BulkExtensions`. Целевая латентность: < 2 ms на batch из 100.

### Batch dispatch

Диспетчер забирает порцию (100–500), использует channel для параллельной отправки в transport. Outbox state переводится в `Dispatched` после подтверждения приёма.

## Inbox

```sql
create table avto_inbox_messages (
    message_id uuid not null,
    consumer_id text not null,
    tenant_id text null,
    received_at timestamptz not null,
    consumed_at timestamptz not null,
    message_type text not null,
    schema_name text not null,
    schema_version int not null,
    correlation_id text null,
    primary key (message_id, consumer_id)
);
```

### Retention

- Default: 7 дней для команд, 30 дней для событий.
- Per-stream политика через конфиг.
- Cleanup job вычищает старые маркеры batch-ами.
- Не удалять раньше broker retention window, иначе теряется гарантия дедупликации при replay.

### Idempotency key vs inbox

Idempotency key — это явный ключ из payload (`paymentId`, `orderId`), который handler проверяет самостоятельно. Inbox — это инфраструктурный marker по `message_id + consumer_id`. Они дополняют друг друга:

- Inbox: защищает от дублей на уровне брокера (один message_id, повторная доставка).
- Idempotency key: защищает handler, если один и тот же business факт приходит в разных message_id (eventual consistency across sources).

### API

```csharp
public interface IAvtoInbox
{
    ValueTask<InboxCheckResult> CheckAsync(AvtoEnvelope envelope, AvtoConsumerIdentity consumer, CancellationToken ct);
    ValueTask RecordAsync(AvtoEnvelope envelope, AvtoConsumerIdentity consumer, CancellationToken ct);
}
```

## Saga store

```sql
create table avto_sagas (
    id text primary key,
    saga_type text not null,
    tenant_id text null,
    correlation_id text not null,
    state bytea not null,        -- serialized saga state
    version bigint not null,     -- optimistic concurrency
    status text not null,        -- Active, Completed, TimedOut, Faulted
    created_at timestamptz not null,
    updated_at timestamptz not null,
    completed_at timestamptz null
);

create index ix_sagas_correlation on avto_sagas (saga_type, correlation_id) where status = 'Active';
```

### Concurrency

- Update: `update avto_sagas set version = version + 1, state = ... where id = ? and version = ?`.
- На конфликте — retry handler с jitter.
- Для hot keys: partition by `correlation_id` и закрепить за конкретным воркером.

## Workflow store

```sql
create table avto_workflow_instances (
    id text primary key,
    workflow_type text not null,
    tenant_id text null,
    status text not null,        -- Running, Completed, Failed, Cancelled, ContinuedAsNew
    state_snapshot bytea null,
    version bigint not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    completed_at timestamptz null
);

create table avto_workflow_history (
    workflow_id text not null,
    sequence bigint not null,
    event_type text not null,
    payload bytea not null,
    trace_id text null,
    created_at timestamptz not null,
    primary key (workflow_id, sequence)
);

create table avto_workflow_timers (
    workflow_id text not null,
    fire_at timestamptz not null,
    sequence bigint not null,
    primary key (workflow_id, sequence)
);
```

### Snapshot

Snapshot сохраняется по порогу: каждые N events или по объёму state. Snapshot — это детерминированный сериализованный state. На replay после snapshot, history до snapshot не воспроизводится.

### Continue-as-new

При превышении размера history workflow останавливается с `ContinuedAsNew`, и запускается новая инстанция с усечённой историей и текущим snapshot.

## Event store

```sql
create table avto_event_streams (
    stream_name text not null,
    version bigint not null,
    envelope_id uuid not null,
    event_type text not null,
    schema_version int not null,
    created_at timestamptz not null,
    tenant_id text null,
    correlation_id text null,
    causation_id text null,
    trace_id text null,
    primary key (stream_name, version)
);
```

- Insert: проверка `expected_version` в той же транзакции.
- Если outbox и event store в одной БД: transactional outbox через `avto_outbox_messages` пишется в той же транзакции с event store row.
- Если разные БД: 2-phase через `envelope_id` reconciliation process.

## Projection checkpoint

```sql
create table avto_projection_checkpoints (
    projection_name text not null,
    shard text not null,
    position text not null,
    state bytea null,
    updated_at timestamptz not null,
    primary key (projection_name, shard)
);
```

Checkpoint сохраняется после commit event batch + read model update.

## Scheduled store

```sql
create table avto_scheduled_messages (
    id uuid primary key,
    envelope_id uuid not null references avto_envelopes(id) on delete cascade,
    scheduled_at timestamptz not null,
    state text not null,         -- Scheduled, Claimed, Dispatched, Cancelled
    locked_by text null,
    locked_until timestamptz null,
    created_at timestamptz not null,
    attempt_count int not null default 0
);
```

Scheduler выбирает строки с `scheduled_at <= now()` и `state = 'Scheduled'`, переводит в `Claimed`, передаёт envelope в outbox, переводит в `Dispatched`.

## Dead-letter

```sql
create table avto_dead_letters (
    id uuid primary key,
    envelope_id uuid not null references avto_envelopes(id) on delete cascade,
    reason text not null,
    endpoint text not null,
    exception_type text null,
    exception_message text null,
    stack_trace_hash text null,
    attempt_count int not null,
    failed_at timestamptz not null,
    replayed_at timestamptz null,
    payload_snapshot bytea null  -- optional, sensitive systems use pointer instead
);
```

- При `payload_snapshot = null` хранится только `envelope_id`, а payload восстанавливается из `avto_envelopes` или external claim check.
- Replay создаёт новую доставку, оригинальный `message_id` сохраняется.

## Schema migrations

CLI:

```bash
dotnet avto migrate --store orders --target-version latest
dotnet avto migrate status --store orders
dotnet avto migrate baseline --store orders --version 0.0.0
```

Миграции AvtoBus таблиц не должны конфликтовать с миграциями приложения. Рекомендуется:

- AvtoBus таблицы — в отдельной schema (`avtobus` в PostgreSQL, `avtobus` в SQL Server).
- Миграции AvtoBus поставляются как SQL-скрипты и через EF Core `Migration` пакет.
- Приложение мигрирует свою schema отдельно.
- Совмещённый запуск через `dotnet ef database update` + `dotnet avto migrate` в pre-deploy hook.

## Schema versioning самого store

- Версия store schema отслеживается в `avto_store_version` (один row per store).
- Outbox/inbox/saga/workflow/event store версионируются независимо.
- AvtoBus minor upgrade может вводить backwards-compatible change, не требующий manual migration.
- Major upgrade может требовать offline migration.

## Storage backends

| Backend | Поддержка | Ограничения |
| --- | --- | --- |
| PostgreSQL | primary | best support for skip locked, advisory locks, jsonb |
| SQL Server | primary | readpast + updlock, нет skip locked, поэтому batch locking + version required |
| EF Core (generic) | supported | abstraction over PostgreSQL/SQL Server, не рекомендуется для hot path |
| MongoDB | preview | не поддерживает strict transactions поверх standalone |
| Cosmos DB | preview | ограничения по размеру document, separate SQL API model |
| Redis | preview | только в pair с persistent store; outbox в Redis + main event store в PG |

## Concurrency model

- Outbox dispatcher: много воркеров, skip locked.
- Inbox: один writer per (message_id, consumer_id). Уникальный PK гарантирует.
- Saga: optimistic concurrency на version.
- Workflow: optimistic concurrency на version + state machine guarantee.
- Projection: checkpoint serialized per shard, can be parallelized across shards.

## Observability store

AvtoBus tables должны быть инструментированы:

- pg_stat_statements или SQL Server DMV для slow query.
- Метрики по количеству строк в каждом state (avtobus_outbox_pending, avtobus_inbox_total, avtobus_sagas_active, avtobus_workflow_running).
- Lag = `now() - min(created_at) where state = 'Pending'`.
- Cleanup job: emit metric по количеству удалённых rows per table.

## Backfill и replay

### Replay of dead-letter

```text
1. Operator selects dead-letter id
2. AvtoBus checks current code/schema compatibility
3. Optional upcast
4. New envelope is created with same message_id
5. Enqueue в outbox (или прямо в transport при emergency)
6. Audit log записан
```

### Backfill of new consumer

```text
1. New consumer endpoint created
2. Определена start_position: from-beginning, from-now, from-offset, from-timestamp
3. AvtoBus вычитывает из source (broker или event store) и enqueue в новый consumer queue
4. Rate limit чтобы не догнать upstream
5. Checkpoint сохраняется per shard
```

## Failure recovery

| Failure | Recovery |
| --- | --- |
| DB connection drop во время commit | retry с backoff, idempotency на PK |
| Outbox dispatcher crash после lock | lock TTL истекает, row возвращается в Pending |
| Inbox insert failure после business commit | retry при следующей доставке; бизнес state дубль возможен → handler должен быть идempotентен через idempotency key |
| Saga conflict | retry с jitter, после N retries — DLQ |
| Workflow history append failure | workflow переходит в Faulted, ручное восстановление |
| Projection DB unavailable | projection pause, retry; resume без потери events благодаря checkpoint |
| Outbox queue растёт быстрее dispatcher | alert, scale dispatcher replicas, проверить downstream |
