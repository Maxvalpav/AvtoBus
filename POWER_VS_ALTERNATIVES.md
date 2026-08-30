# AvtoBus Power vs Альтернативы — как быть мощнее всех (2026)

> Источники: masstransit.io (v9 commercial $400/мес), codingdroplets.com/masstransit-vs-nservicebus-2026, code-maze.com/dotnet-wolverine-library (Wolverine MIT), code-maze.com/aspnetcore-comparison-of-rebus-nservicebus-and-masstransit (Rebus MIT), github.com/dotnetcore/CAP, relatedrepos.com/gh/MassTransit/MassTransit, visualstudiomagazine.com (Wolverine/MassTransit/NServiceBus paid, Wolverine/Dapr free).

## Таблица превосходства

| Критерий | MassTransit v9 | NServiceBus | Wolverine 6.x | CAP | Rebus | Brighter/EasyNetQ/Dapr | **AvtoBus Power 7-Power** |
|---|---|---|---|---|---|---|---|
| **Лицензия** | Commercial source-available, v8 Apache2 EOL 12.2026 | Commercial per-endpoint, Community 3 ep/10k msg/день | **MIT** (MediatR 13+ dual RPL) | MIT (CAP) | **MIT** | MIT/Apache | **MIT** — как Wolverine/Rebus, не как MT/NSB |
| **Цена прод** | $400/мес (> $1M revenue, may-qualify) | $endpoint/день + лимит msg | 0 | 0 | 0 | 0 | **0** |
| **Вызов** | \IPublishEndpoint/IPublish\ + \ISendEndpoint\ + \IBus\ 3 API, \IConsumer<T>.Consume\ | \EndpointConfiguration\ + \IHandleMessages<T>\ + ServicePulse | \plain method\ code-gen (без интерфейса) | \ICapPublisher.Publish\ + outbox manual | \IBus.Send/Publish\ \IHandleMessages<T>\ | \IAmACommandProcessor\ / \IBus/PubSub\ | **1 API** \IBus.PublishAsync/SendAsync/RequestAsync/ScheduleAsync\ + \IMessageSession\ scoped + **plain method** \static Task Handle(T, ConsumeContext)\ source-gen (Wolverine-DX + MT совместимость) |
| **Генерация/AOT** | Reflection + Automatonymous | Reflection | **Source-gen**, AOT pillar #2746 | Reflection | Reflection | Reflection | **Source-gen + Analyzers AVB001-060 + JsonContext AOT** — как Wolverine, сильнее MT/NSB/CAP |
| **Outbox/Inbox** | \AddEntityFrameworkOutbox\ + \UseBusOutbox\ (row locking) | Outbox SQL (bridge) | **Durable outbox/inbox** + \UseDurableInbox/LocalQueues\ + \MessageIdentity.IdAndDestination\ + \OutboxStaleTime\ | **CAP outbox** (EF Core/MySQL/PG) с Dashboard | Outbox manual | Brighter outbox, Dapr sidecar | **Авто-Outbox** \AvtoBus.Outbox.EfCore\ + \Sql SKIP LOCKED\ + \Durability.PostgreSql\ lease (фикс sol DLQ) + **CAP-совместимый batch** — объединяет MT+Wolverine+CAP |
| **Транспорты** | RabbitMQ/ASB/SQS/ActiveMQ/SQL/PG/InMemory/Kafka | ASB/RabbitMQ/SQS/SQL/MSMQ/Storage + **Bridge** | RabbitMQ/ASB/SQS/Kafka (+Martен) | RabbitMQ/Kafka/ASB/NATS | RabbitMQ/ASB/SQS/SQL/PG | RabbitMQ/Redis/Kafka/gRPC/Dapr pub/sub | **7 реальных**: InMemory/RabbitMQ/Kafka/NATS/Redis/Sql/ASB + LocalQueue bounded — шире NSB, как MT |
| **Sagas/Workflows** | Automatonymous state machine | Saga + TimeoutManager | Saga + durable \[Transactional]\ handler \IDocumentSession\ | Saga manual | Saga idempotent | Brighter saga, Elsa Core | **Sagas + Scheduling + EventSourcing (Axon) + Workflow durable timer/activity/history** \AvtoBus.Workflow\ + **Streams** \IStateStore/Window\ (Kafka Streams) — сильнее всех |
| **Надежность** | Retry interval/exponential + redelivery | Immediate+Delayed + ServicePulse | Retry + inbox + durable local queues | Retry + Dashboard | Simple retry | Polly/Hangfire | **Immediate+Delayed+jitter + RetryBudget + CircuitBreaker + Bulkhead AIMD + CanaryProbe + TrafficAnomaly + OrderedBy partition** — как Polly+NSB |
| **Stream/Events** | EventStore нет | нет | Marten events | нет | нет | Orleans/Akka streams | **EventSourcing + Streams Window + projection** — как Marten+Kafka Streams |
| **Безопасность** | body encryption header plain | property-based encryption | TLS | TLS | \EnableEncryption(AES)\ body only | TLS | **HMAC-SHA256 + AES-256-GCM per-field [Encrypted] + RBAC [BusAuthorize] + PII + tenant isolation** — сильнее Rebus/NServiceBus |
| **Observability** | OTel | ServicePulse/ServiceInsight + OTel | OTel + Marten | Dashboard | Logging | HealthChecks | **OTel + EventSource + Meter \AvtoBus\ + Grafana + Jaeger + Prometheus + Health \AvtoBus\** — как Wolverine + CAP Dashboard |
| **Операции** | — | ServicePulse/Control | Aspire | CAP Dashboard | — | Dapr sidecar | **Dashboard + EventCatalog + AsyncAPI 3.0 + Cli dlq list/replay + Aspire + K8s/KEDA/HPA/PDB + Terraform** — как CAP+NSB+Dapr |
| **Производительность** | ~0.5M msg/s | — | Mediator perf + code-gen | — | lean | — | **Zero-alloc RingBuffer 64b + FrozenDictionary + SimdHeaderParser + PooledEnvelope + BenchmarkDotNet** — как 5/ |

## Что делаем лучше каждой

**vs MassTransit:** MIT vs $400, 1 \IBus\ vs 3, plain method AOT vs \IConsumer\, 0$ vs коммерция. Берем \AvtoBus.Generators\ + \LocalQueue\.
**vs NServiceBus:** 0$ vs per-endpoint, 7 транспортов vs 4, Bridge делаем через \TransportRegistry\ + \RoutingTable.ToQueue(...).Via()\, ServicePulse заменяем Dashboard+Grafana.
**vs Wolverine:** добавляем 3 доп транспорта (Redis/Sql/ASB already), Security per-field, Multitenancy, Streams Window — чего у Wolverine нет. Сохраняем Wolverine-DX \static Handle\.
**vs CAP:** CAP — только outbox+Dashboard, мы — полный bus + sagas + streams + inbox IdAndDestination.
**vs Rebus/Brighter:** Rebus lean — мы lean + saga + outbox + analyzers; Brighter — берем \IBatchDispatcher\ идею + добавляем Workflow.
**vs Dapr:** Dapr sidecar — мы in-process без sidecar, быстрее, с AOT.

## Что добавить в 7-Power чтобы закрыть все (чеклист)

- [x] Импорт fable Workflow/Streams уже в \src/\
- [ ] Починить \PostgresOutboxLeaseStore\ из sol-import (DLQ FAIL) — 1 день
- [ ] Добавить \Transport Bridge\ (как NServiceBus) — \AvtoBus.Sql\ <-> RabbitMQ — 2 дня
- [ ] Добавить \CAP Dashboard\ совместимый UI в \AvtoBus.Dashboard\ — 2 дня
- [ ] Добавить \Rebus EnableEncryption\ совместимый флаг \UseEncryption(key)\ — 1 день
- [ ] Бенчмарк vs MT/Wolverine/Rebus (BenchmarkDotNet) публика latency — 1 день
- [ ] Миграция гайды \able-import/13-migration-cookbook.md\ -> \docs/\ — 1 день

После этого — единственная MIT-альтернатива которая бьет всех по фичам и цене.
