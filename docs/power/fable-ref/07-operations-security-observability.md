# AvtoBus: operations, security и observability

Этот документ описывает production-grade требования, которые должны быть отражены в AvtoBus 1.0 и далее. Цель - оператор может понять состояние системы без чтения кода, а безопасность не является опциональной надстройкой.

## Observability model

### Три сигнала по умолчанию

AvtoBus обязан из traces, metrics и logs с единой корреляцией:

- Trace: `MessageId`, `CorrelationId`, `CausationId`, `ConversationId`, `TenantId`, `EndpointName`, `HandlerName`.
- Metric labels: `message_type`, `schema_version`, `endpoint`, `transport`, `outcome`, `tenant` если разрешено.
- Log fields: те же плюс `attempt`, `exception_type`, `dead_letter_reason` где применимо.

### ActivitySource и Meter names

```text
ActivitySource: AvtoBus
Meter: AvtoBus
```

Основные spans:

```text
avtobus.send
avtobus.publish
avtobus.invoke
avtobus.receive
avtobus.handler
avtobus.outbox.save
avtobus.outbox.dispatch
avtobus.inbox.check
avtobus.saga.load
avtobus.saga.save
avtobus.workflow.step
avtobus.projection.batch
avtobus.stream.window
```

### Metrics catalog

| Metric | Type | Labels | Назначение |
| --- | --- | --- | --- |
| `avtobus_messages_total` | counter | message_type, endpoint, transport, outcome | throughput |
| `avtobus_message_duration_seconds` | histogram | message_type, endpoint, handler | latency |
| `avtobus_handler_errors_total` | counter | message_type, exception_type | error rate |
| `avtobus_retry_total` | counter | message_type, endpoint, retry_kind | retry pressure |
| `avtobus_dead_letter_total` | counter | endpoint, reason | poison traffic |
| `avtobus_outbox_pending` | gauge | store, transport | producer backlog |
| `avtobus_outbox_lag_seconds` | gauge | store | oldest undispatched |
| `avtobus_inbox_duplicate_total` | counter | consumer | redelivery pressure |
| `avtobus_endpoint_queue_depth` | gauge | endpoint, transport | KEDA/HPA scaling |
| `avtobus_endpoint_consumer_lag` | gauge | endpoint, partition | consumer scaling |
| `avtobus_scheduled_pending` | gauge | store | scheduler backlog |
| `avtobus_saga_active` | gauge | saga_type | long-running process count |
| `avtobus_workflow_active` | gauge | workflow_type | workflow pressure |
| `avtobus_projection_lag_seconds` | gauge | projection, shard | read model freshness |
| `avtobus_schema_incompatible_total` | counter | schema_name | contract drift |
| `avtobus_payload_bytes` | histogram | message_type, direction | payload size monitoring |
| `avtobus_transport_errors_total` | counter | transport, operation | infra health |

### Health checks

```text
avtobus-transport-{name}
avtobus-outbox-{store}
avtobus-inbox-{store}
avtobus-scheduler
avtobus-projection-{name}
avtobus-schema-registry
avtobus-workflow-worker
```

Health statuses:

- Healthy: lag and error rate within thresholds.
- Degraded: lag above warning threshold or retry rate above baseline.
- Unhealthy: transport unreachable, store failing, dead-letter growth above threshold.

## Prometheus и Grafana

### Prometheus scraping

AvtoBus dashboard endpoint может отдавать `/metrics` через `OpenTelemetry.Exporter.Prometheus.AspNetCore` или через Aspire defaults.

### Grafana templates

AvtoBus должен поставлять JSON dashboard templates:

- Overview: throughput, error rate, p95 latency, dead letters.
- Reliability: outbox lag, inbox duplicate, retry counts.
- Endpoints: per-endpoint queue depth, consumer lag, concurrency.
- Sagas and workflows: active counts, stuck timers, failed activities.
- Projections: lag, batch duration, rebuild status.
- Schema: version distribution and incompatible attempts.

### Alerting rules примеры

```yaml
- alert: AvtoBusOutboxLagHigh
  expr: avtobus_outbox_lag_seconds > 300
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Outbox dispatch lag is above 5 minutes"

- alert: AvtoBusDeadLetterGrowing
  expr: rate(avtobus_dead_letter_total[10m]) > 0
  for: 10m
  labels:
    severity: critical

- alert: AvtoBusConsumerLagHigh
  expr: avtobus_endpoint_consumer_lag > 100000
  for: 10m
  labels:
    severity: warning

- alert: AvtoBusSchemaIncompatible
  expr: increase(avtobus_schema_incompatible_total[1h]) > 0
  labels:
    severity: critical
```

## SLO и SLI

Рекомендованные SLI:

- Delivery success rate: `1 - dead_letter_total / messages_total` per endpoint.
- End-to-end latency: publish-to-handle p95 для critical events.
- Outbox freshness: p99 `outbox_lag_seconds`.
- Projection freshness: p99 `projection_lag_seconds`.

Рекомендованные SLO примеры:

- 99.9% delivery success over 30 days.
- p95 publish-to-handle latency менее 2s для operational events.
- Outbox lag p99 менее 30s.

## Security

### Transport security

- TLS для всех broker connections.
- mTLS для RabbitMQ, Kafka, NATS, gRPC где поддерживается.
- Certificate rotation через стандартные .NET `SslOptions` и secret manager.
- Не логировать connection strings и SAS tokens.

### Message security

- Optional payload encryption per message type: `policies.For<PaymentEvent>().EncryptPayload()`.
- Optional signing для externally received messages: HMAC или asymmetric signature.
- Key material через `IConfiguration`, Azure Key Vault, AWS Secrets Manager, HashiCorp Vault adapter.
- Key rotation: encrypted payload header хранит `key-id`.

### Authorization

- Per-message authorization policy:

```csharp
bus.Policies(policies => policies
    .For<SubmitOrder>()
    .RequireAuthorization("orders.write"));
```

- Authorization context берется из HTTP user, service identity, mTLS certificate или JWT в envelope headers.
- Для async messages: service identity или signed token в header, а не user token, если user session не должен жить долго.

### Dashboard security

- Dashboard не должен быть публичным.
- `MapAvtoBusDashboard()` требует authorization policy.
- Role-based actions: read-only, replay, rebuild projection, schema approve.
- Audit log для всех write actions.
- Payload inspection может быть disabled для sensitive namespaces.

### PII, GDPR и retention

- Attribute или policy marking: `[AvtoPii]`, `policies.MaskPayloadInLogs()`.
- PII fields могут быть encrypted at rest в outbox/dead-letter/event store.
- Right-to-be-forgotten: для event store нужно поддерживать redaction через tombstone + projection rebuild, если legal requirement требует физического удаления.
- Retention policies per stream/outbox/inbox/dead-letter.
- Data residency: multi-region stores и transport selection per tenant.

### Payload size и abuse protection

- Max payload size per endpoint.
- Max header size.
- Max header count.
- Rate limiting per tenant/client.
- Circuit breaker per downstream dependency через Polly v8 resilience pipeline.

## Large payload и Claim Check

Проблема: сообщения более 1-5 MB плохо подходят для broker headers/queue limits и увеличивают outbox DB load.

AvtoBus solution:

- `AvtoBus.Storage.Blob` adapter: Azure Blob, S3, filesystem, PostgreSQL large object.
- Policy:

```csharp
policies.ForAllMessages()
    .UseClaimCheck(threshold: 256.Kilobytes(), store: "blob");
```

- Envelope хранит pointer, content type, hash и size.
- Consumer автоматически materializes payload через registered claim check provider.
- Retention для blobs tied to inbox/outbox retention.

## Graceful shutdown и drain

Последовательность shutdown:

1. Stop accepting new HTTP requests для producer endpoints, если ASP.NET Core shutdown triggered.
2. Stop receiving new messages from transports.
3. Finish in-flight handlers within configured drain timeout.
4. Flush outbox dispatcher batches.
5. Flush telemetry.
6. Dispose transports and stores.

Configuration:

```csharp
bus.Hosting(hosting => hosting
    .DrainTimeout(TimeSpan.FromSeconds(30))
    .ShutdownTimeout(TimeSpan.FromSeconds(45))
    .CancelInFlightAfter(TimeSpan.FromSeconds(25)));
```

## Hot reload и dynamic configuration

- Route changes require restart для predictability.
- Policy thresholds, retry counts, concurrency и feature flags могут reload через `IOptionsMonitor`.
- Transport credentials reload через secret manager.
- Schema registry cache reload без restart.

## KEDA и autoscaling

Пример ScaledObject для RabbitMQ endpoint:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-worker
spec:
  scaleTargetRef:
    name: orders-worker
  minReplicaCount: 2
  maxReplicaCount: 50
  triggers:
  - type: rabbitmq
    metadata:
      queueName: orders.commands
      queueLength: "50"
```

Пример для Kafka consumer lag:

```yaml
triggers:
- type: kafka
  metadata:
    bootstrapServers: kafka:9092
    consumerGroup: orders-api
    topic: orders.events.v1
    lagThreshold: "1000"
```

AvtoBus CLI:

```bash
dotnet avto keda export --endpoint orders --min 2 --max 50 --queue-length 50
```

## Disaster recovery

### Backup

- Outbox/inbox/saga/workflow/event store backups через DB-native tools.
- Blob claim check backup через storage lifecycle policies.
- Schema registry export через `dotnet avto schemas export`.

### Restore и replay

- Restore DB from backup.
- Replay dead letters после code fix.
- Rebuild projections from event store.
- Resume workflows from history.

### Cross-region

- Active-passive: secondary region consumes same event topics with separate consumer group for warm standby.
- Active-active: требует partitioned tenant routing и conflict-free aggregate ownership.
- AvtoBus должен поддерживать tenant-aware routing для multi-region deployment.

## Multi-tenant isolation strategies

Tenant isolation — не одна настройка, а набор слоёв. Выбор стратегии определяется compliance требованиями.

| Стратегия | Когда подходит | Trade-off |
| --- | --- | --- |
| Single DB, tenant_id column | low-risk tenants, dev, SMB | shared resource contention, риск утечки между tenants |
| Single DB, row-level security (PostgreSQL RLS) | medium-risk, single-tenant regulation | RLS overhead, нужен SET LOCAL на каждое connection |
| Schema-per-tenant | high-risk, regulated industries | schema migrations scale O(N) per tenant |
| Database-per-tenant | ISO 27001, banking, healthcare | max isolation, max operational cost |

AvtoBus рекомендации:

- В `avto_envelopes`, `avto_outbox_messages`, `avto_sagas` всегда `tenant_id` column.
- Per-tenant routing: `policies.ForTenant(t).RouteToEndpoint("orders.tenant.{t}")` где endpoint может быть отдельная queue/topic.
- Per-tenant durability stores: `durability.ForTenant(t).UsePostgreSql(connectionString)`.
- Per-tenant encryption keys: `claims.EncryptForTenant(t)`.
- Per-tenant quota: rate limit, max outbox messages, max payload size.
- Per-tenant outbox dispatcher: dedicated worker pool для premium tier.

CLI:

```bash
dotnet avto tenants list
dotnet avto tenants create --id acme --isolation database --db-postgres "..."
dotnet avto tenants set-policy --id acme --policy retention 30d
dotnet avto tenants inspect --id acme
```

## Capacity planning

Rules of thumb:

- Outbox batch size: 100-500 messages.
- Outbox dispatcher polling: 100-500ms.
- Consumer prefetch: зависит от handler latency и broker; начать с 256-1024.
- Max concurrency: CPU-bound handlers near core count, I/O-bound handlers higher.
- Partition parallelism: bounded by partition count для Kafka.
- Projection batch: 100-2000 events, checkpoint after commit.

## Production checklist

- [ ] OpenTelemetry exporter configured.
- [ ] Health checks exposed and monitored.
- [ ] Dead-letter alerts configured.
- [ ] Outbox lag alerts configured.
- [ ] Consumer lag alerts configured.
- [ ] Dashboard behind authorization.
- [ ] Replay actions audited.
- [ ] PII fields marked and masked.
- [ ] Retention policies set for outbox/inbox/dead-letter/event store.
- [ ] Claim check configured for large payloads.
- [ ] KEDA or HPA configured with AvtoBus metrics.
- [ ] Backup/restore tested.
- [ ] Schema breaking changes fail CI.
- [ ] Contract tests run between producers and consumers.
- [ ] Graceful shutdown drain tested under load.
- [ ] TLS/mTLS enabled for transports.
- [ ] Secrets rotated and not logged.
- [ ] Multi-region strategy documented where required.
