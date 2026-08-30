# AvtoBus Power — мощнее MassTransit / NServiceBus / Wolverine

> База: \7-Power/\ = \3/\ (26 проектов, 360 cs, 406 тестов, 0W) + \able\ (16 доков, Workflow/Streams) + \sol\ (Postgres lease фиксы) + \Eda\ (Bank 20 сервисов как e2e). Цель — MIT-альтернатива коммерческому MassTransit v9 ($400/мес).

## Почему мощнее MassTransit (2026)

| Масса | MassTransit v9 | AvtoBus Power |
|---|---|---|
| Лицензия | Commercial v9, v8 EOL 12.2026 | **MIT** |
| Вызов | \IPublishEndpoint.Publish\ + \ISendEndpoint\ + \IBus\ 3 API | **Единый** \IBus.PublishAsync/SendAsync/RequestAsync/ScheduleAsync\ + \IMessageSession\ scoped для Outbox |
| Consumer | \IConsumer<T>.Consume\ обязателен | \IConsumer<T>.ConsumeAsync\ **или** \static Task Handle(T, ConsumeContext)\ (source-gen, без интерфейса — Wolverine-стиль, AOT) |
| Генерация | Reflection | **Source Generators** \AvtoBus.Generators\ + \AvtoBus.Analyzers\ (AVB001-060), \JsonSerializerContext\ AOT |
| Outbox | \EfCoreOutbox\ опционально | **Транзакционный по умолчанию** \AvtoBus.Outbox.EfCore\ + \AvtoBus.Sql\ SKIP LOCKED + \Durability.PostgreSql\ lease (исправлен Max_attempts из sol) |
| Транспорты | RabbitMQ/ASB/SQS/ActiveMQ | **7 реальных**: InMemory/RabbitMQ/Kafka/NATS/Redis/Sql/Azure SB + LocalQueue bounded (fable Workflow/Streams + 3/Sql) |
| Workflows | Automatonymous Saga | **Sagas + Workflow (durable timer/activity/history)** \src/AvtoBus.Workflow\ + **Streams** \IStateStore/Window\ (Kafka Streams-стиль) + EventSourcing (Axon) |
| Надежность | Retry + Redelivery | Immediate/Delayed retry + jitter + **RetryBudget** + **CircuitBreaker** + **Bulkhead (AIMD)** + **CanaryProbe** + **TrafficAnomalyDetector** + Partition \OrderedBy\ |
| Безопасность | нет | **HMAC-SHA256 signing + AES-256-GCM + per-field [Encrypted] + RBAC [BusAuthorize] + PII masking + tenant isolation** \src/AvtoBus.Security+Multitenancy\ |
| Observability | OTel базово | **OTel + AvtoBusEventSource + Meter \AvtoBus\ + Grafana + Jaeger + Prometheus** \uild/docker-compose.dev.yml\, \BusTelemetry\, \IConsumerLagProvider\ |
| Операции | ServicePulse  | **Dashboard + EventCatalog + AsyncAPI 3.0 + Cli \dlq list/replay\ + Aspire + K8s/KEDA/HPA/PDB + Terraform** |
| Производительность | ~0.5M msg/s | **Zero-alloc** \RingBufferCursor 64b aligned\, \FrozenDictionary\, \SimdHeaderParser\, \PooledEnvelope\ (из 5/) + BenchmarkDotNet |
| Тесты | harness | **9 test-проектов, 400+ тестов, Testcontainers, InMemory conformance** + fable durability tests |

## Что собрано в 7-Power

- **Исходник**: копия \3/\ (самый зрелый) — \AvtoBus.slnx\ 0W/0E
- **Импорт fable**: \docs/fable-import/\ (15 md) — архитектура, генераторы deep-dive, testing-guide, migration-cookbook; \src/AvtoBus.Workflow\ + \src/AvtoBus.Streams\ + \src/AvtoBus.Durability.PostgreSql\
- **Импорт sol**: \docs/sol-import/\ — рабочие \PostgresOutboxLeaseStore.cs\, \DlqService.cs\, \V1__initial_avtobus_schema.sql\ (фиксят FAIL \Max_attempts_message_moved_to_dlq\)

## Роадмап чтобы быть мощнее (4 недели)

**Неделя 1 — Ядро (из 27-gap P0):**
- [ ] Починить DLQ из sol (\PostgresDlqStore\ -> \3/src/AvtoBus.Core/Runtime/DlqReader.cs\)
- [ ] Добавить \AvtoBus.Workflow\ + \AvtoBus.Streams\ в \AvtoBus.slnx\ + \Directory.Packages.props\
- [ ] Включить \LocalQueueTransport\ bounded + back-pressure тесты

**Неделя 2 — Транспорты + AOT:**
- [ ] Прогнать \34-verification-matrix.md\ — InMemory + RabbitMQ L0-L2 conformance (Testcontainers)
- [ ] \dotnet publish samples/AvtoBus.AotSample -p:PublishAot=true\ 0 warnings
- [ ] Benchmark \publish latency\ vs MassTransit (BenchmarkDotNet.Artifacts)

**Неделя 3 — Релиз:**
- [ ] \dotnet pack\ 29 пакетов (26 + 3 новых) + MinVer/Sourcelink
- [ ] E2E \samples/AvtoBus.Logistics\ 30 сервисов + \Eda/Bank\ 20 сервисов как smoke
- [ ] Доки \22-getting-started.md\ doc-tests

**Неделя 4 — Операции:**
- [ ] Helm/KEDA + Terraform envs + Grafana dashboard JSON из fable \14-reference-sample\
- [ ] Security audit (HMAC + per-field AES) + PII \PerformanceTests\

## Запуск

\\\pwsh
# База уже собирается
dotnet build AvtoBus.slnx -c Release # 0W/0E
dotnet test -c Release --filter Category!=Integration # 292 pass

# После мёрджа Workflow/Streams
dotnet build AvtoBus.slnx -c Release
dotnet publish samples/AvtoBus.QuickStart -c Release
docker compose -f build/docker-compose.dev.yml up -d
\\\

## Сравнение вызова (vs MassTransit выше) — наш DX в 2 строки

\\\csharp
builder.Services.AddAvtoBus(bus => bus
    .UseRabbitMq(o=>o.ConnectionString=\"amqp://...\")
    .UseOutbox<OrderDbContext>()
    .AddConsumersFromAssembly(typeof(Program).Assembly));

await bus.PublishAsync(new OrderPlaced(id, 999));
\\\
vs MassTransit \AddMassTransit(x=>x.UsingRabbitMq(...cfg.ConfigureEndpoints))\ + \IPublishEndpoint\.

