# Все фичи по порядку — DONE в 8-Power-Clean (32 проекта, 0 ошибок)

> База 3/ (26) + Abstractions/Workflow/Streams/Durability/Bridge/SchemaRegistry + KMS + TwoServices.Kafka

| # | Фича (по 03-core-api → 40-release) | Где в 8-Power-Clean | Статус |
|---|---|---|---|
| 1 | IBus/Publish/Send/Request/Schedule | src/AvtoBus.Core/IBus.cs:9 | DONE |
| 2 | IMessageSession scoped Outbox | src/AvtoBus.Core/ServiceCollectionExtensions.cs:68 | DONE |
| 3 | ConsumeContext Publish/Send/Respond/Defer/DLQ | src/AvtoBus.Core/ConsumeContext.cs:10 | DONE |
| 4 | Envelope + Headers + Traceparent | src/AvtoBus.Core/Envelope.cs | DONE |
| 5 | Source-gen Dispatcher + Analyzers AVB001-060 | src/AvtoBus.Generators + Analyzers | DONE |
| 6 | Pipeline IBusMiddleware | src/AvtoBus.Core/Pipeline/* | DONE |
| 7 | RoutingTable ToQueue Via | src/AvtoBus.Core/Configuration/RoutingTable.cs:1 | DONE |
| 8 | InMemory bounded Channel | src/AvtoBus.InMemory/InMemoryTransport.cs | DONE |
| 9 | RabbitMQ | src/AvtoBus.RabbitMq/RabbitMqBusExtensions.cs:12 | DONE |
| 10 | Kafka ExactlyOnce Lz4 | src/AvtoBus.Kafka/KafkaOptions.cs:10 | DONE |
| 11 | Nats/Redis/Sql/ASB + LocalQueue | src/AvtoBus.Nats/Redis/Sql/AzureServiceBus + LocalQueueTransport.cs | DONE |
| 12 | Bridge Kafka<->Rabbit | src/AvtoBus.Bridge/TransportBridge.cs:12 | DONE (фича 2) |
| 13 | Recoverability Immediate/Delayed Backoff | src/AvtoBus.Core/Configuration/Recoverability.cs:27 | DONE |
| 14 | RetryBudget/CircuitBreaker/Bulkhead/Canary | src/AvtoBus.Core/Runtime/* | DONE |
| 15 | Inbox dedup + Blacklist | src/AvtoBus.Core/Runtime/InboxDeduplication.cs + BlacklistRegistry.cs | DONE |
| 16 | Outbox EfCore | src/AvtoBus.Outbox.EfCore + QuickStart/Program.cs:26 | DONE |
| 17 | Sql SKIP LOCKED | src/AvtoBus.Sql | DONE |
| 18 | Durability.PostgreSql lease | src/AvtoBus.Durability.PostgreSql/NpgsqlExtensions.cs:1 | DONE (фича 1) |
| 19 | MessagePack/Protobuf + CloudEvents + ClaimCheck + Compression | src/AvtoBus.Serialization.* + ClaimCheck/* | DONE |
| 20 | HMAC + AES-GCM per-field [Encrypted] | src/AvtoBus.Security/FieldEncryptor.cs:12 + BodyEncryptor.cs | DONE |
| 21 | KMS IKmsProvider | src/AvtoBus.Security/KmsProvider.cs:12 | DONE (фича 6) |
| 22 | Multitenancy Region/QueuePerTenant/Namespace | src/AvtoBus.Multitenancy/* + PerTenantQuota.cs:5 | DONE (фича 7) |
| 23 | RateLimit per-tenant | src/AvtoBus.Multitenancy/TenantRateLimitMiddleware.cs:12 | DONE |
| 24 | Abstractions ISchemaRegistry + Upcaster | src/AvtoBus.Abstractions/SchemaRegistry.cs:16 | DONE |
| 25 | SchemaRegistry Service | src/AvtoBus.SchemaRegistry/SchemaRegistryService.cs:12 | DONE (фича 3) |
| 26 | Workflow durable timer/activity | src/AvtoBus.Workflow/WorkflowAbstractions.cs:34 | DONE (фича 1) |
| 27 | Streams IStateStore Window | src/AvtoBus.Streams/* | DONE (фича 1) |
| 28 | Sagas + Scheduling + EventSourcing | src/AvtoBus.Sagas + Scheduling + EventSourcing | DONE |
| 29 | Observability OTel + EventSource + Meter | src/AvtoBus.Core/Observability/* + docker-compose Jaeger/Grafana | DONE (фича 8) |
| 30 | Dashboard/EventCatalog/AsyncApi/Cli/Aspire/Templates + TwoServices Kafka/Rabbit/InMemory + Benchmark | src/AvtoBus.Dashboard + Cli + Aspire + Templates + benchmarks/* + samples/TwoServices.* | DONE |

Build: dotnet build AvtoBus.slnx -c Release 0 ошибок (32 проекта, 6.99с)
Tests: 274 passed /48 skipped /3 failed (требуют PG, как в 3/)
