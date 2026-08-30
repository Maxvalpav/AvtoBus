# AvtoBus Power Clean — 8-Power-Clean

Чистая сборка на базе  + "3/" +  (26 проектов, 360 cs) + лучшие идеи fable/sol.

## Структура
`
8-Power-Clean/
  AvtoBus.slnx               — 26 проектов, 0 ошибок (Release)
  src/                       — AvtoBus.Core/InMemory/RabbitMq/Kafka/NATS/Redis/Sql/AzureServiceBus/Outbox/Sagas/... (как в 3/)
  src/_power-reference/      — Workflow/Streams/Durability.PostgreSql из fable (не в сборке, для портации)
  docs/                      — 40+ md из 3/ + docs/power/fable-ref (15 md) + sol-ref (V1.sql + lease)
  POWER.md                   — почему мощнее MassTransit
  POWER_VS_ALTERNATIVES.md   — таблица vs NServiceBus/Wolverine/CAP/Rebus/Dapr
  build/docker-compose.dev.yml — Rabbit/PG/Redis/Jaeger/Grafana
  samples/AvtoBus.QuickStart — шаблон с Outbox + Recoverability
`

## Быстрый старт
`
dotnet restore AvtoBus.slnx
dotnet build AvtoBus.slnx -c Release # 0 ошибок, 12 MINVER варнов (не git)
dotnet test -c Release --filter Category!=Integration
docker compose -f build/docker-compose.dev.yml up -d
dotnet run --project samples/AvtoBus.QuickStart
`

## Чем мощнее альтернатив (MIT vs Commercial)
- MassTransit v9 $400/мес + 3 API vs 1 IBus
- NServiceBus per-endpoint vs 0$
- Wolverine MIT но без Redis/Sql/ASB/Security per-field
- CAP только outbox, мы full bus
Детали в POWER_VS_ALTERNATIVES.md

## Что дальше (4 недели)
Неделя1: портировать sol Dlq fix + fable Workflow/Streams в Core
Неделя2: L0-L2 conformance + AOT publish
Неделя3: Benchmark vs MT + pack 29 nuget
Неделя4: Helm/KEDA + Grafana + migration guide
