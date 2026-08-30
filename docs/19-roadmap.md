# 🗺 AvtoBus — Roadmap

> **Статус: Planning.** Сроки в неделях — плейсхолдеры для оценки объёма, не обязательства. Реальный roadmap появится после закрытия MVP-чеклиста из `FINAL.md`.

## Milestone 0 — Skeleton (2 недели)

**Цель:** «hello world» — публикация и приём одного сообщения через InMemory.

- `AvtoBus.Core`: Envelope, IBus, ConsumeContext, IBusMiddleware, DispatcherRegistry
- `AvtoBus.InMemory`: полный InMemoryTransport
- `AvtoBus.Generators`: минимальный генератор диспетчеров (method-handlers)
- `AvtoBus.Testing`: базовый TestHarness
- 1 sample: WebApi + фон с одним хендлером
- CI: build, test, форматтер, аналитик

Критерий готовности: `dotnet new avtobus-worker && dotnet run` работает за 30 сек.

## Milestone 1 — Reliability Core (4 недели)

- `AvtoBus.RabbitMq`: full transport + автотопология + quorum queues
- Retry-топология (immediate + delayed через TTL-очереди)
- `AvtoBus.Outbox.EfCore`: полный transactional outbox + inbox dedup
- Recoverability middleware (per-exception классификация)
- DLQ + reader API
- OpenTelemetry-интеграция (traces + metrics)
- Аналогов 20+ идей из блоков «Reliability» и «Observability»

Критерий: любое падение (брокер/БД/приложение) не теряет и не задваивает сообщения — доказано в chaos-тестах.

## Milestone 2 — DX Ready (4 недели)

- Полный Source Generator (диагностики AVB001..AVB020, JsonSerializerContext)
- Analyzers + code-fixes
- `AvtoBus.Cli` (dotnet tool): topology / dlq / doctor / dev
- `AvtoBus.Dashboard` (Blazor): очереди, DLQ, реплей, топология-граф
- Templates: `dotnet new avtobus-*`
- Документация с doc-tested примерами
- .NET Aspire интеграция

Критерий: новый разработчик от нуля до отладки прод-инцидента через дашборд — 30 минут.

## Milestone 3 — Sagas & Scheduling (4 недели)

- `AvtoBus.Sagas`: Saga<TState> + оптимистичный SagaStore для EF/Marten
- Стейт-машина DSL с экспортом в Mermaid/BPMN
- Durable-execution runner (Temporal-lite)
- `AvtoBus.Scheduling`: cron, отложенные, лидер-элекшн через advisory locks
- SLA-мониторы и алерты
- Saga test-scenario harness

Критерий: пример «Booking Saga с компенсациями» — 100 строк кода, 15 строк теста.

## Milestone 4 — More Transports (3 недели)

- `AvtoBus.Kafka` (exactly-once транзакции, cooperative-sticky)
- `AvtoBus.AzureServiceBus` (sessions, scheduled enqueue)
- `AvtoBus.Nats` (JetStream pull consumers)
- `AvtoBus.Redis` (Streams + XAUTOCLAIM)
- Conformance-kit прошёл всеми транспортами

Критерий: смена транспорта — только конфиг, ни строчки бизнес-кода.

## Milestone 5 — Event Sourcing (5 недель)

- `AvtoBus.EventSourcing`: EventStore на PostgreSQL, snapshots, upcasters
- Inline / Async / Live проекции
- Реплей и blue/green переключение
- Crypto-shredding + GDPR-отчёт
- Интеграция с шиной (outbox из стора)

Критерий: пример с миллионом событий реплеится < 60 сек, `avtobus es explain` работает.

## Milestone 6 — Production Hardening (3 недели)

- Multi-region (idea 473)
- Мультитенантность полная (уровни A/B/C)
- Подписи сообщений + envelope encryption
- Rate limiting per-principal
- KEDA-скейлеры, chiseled Docker-образы
- SBOM, подписанные NuGet

## Milestone 7 — Streams & AI (5 недель)

- Мини-DSL стрим-процессинга (idea 289)
- AsyncAPI генератор
- Event Catalog как сайт
- AI-инструменты (семантический поиск, upcaster-черновики, incident-summary)
- WASM-плагины

## v1.0 Release Gate

- Все идеи P0 из `02-competitors.md` реализованы
- Conformance-kit: 100% зелёный на 5+ транспортах
- Perf-бенчи: см. `20-benchmarks.md`, все SLO выполнены
- 3 продакшн-пользователя (пилоты)
- Документация: 100% doc-tests зелёные
- SECURITY.md, threat model опубликованы

## Post-v1: сообщество и экосистема

- Program сертификации community-транспортов (idea 450)
- Расширенные шаблоны индустрии (idea 242)
- Регулярные release-cycles: LTS раз в 2 года (совпадает с .NET LTS), STS раз в 6 мес
- Public roadmap + RFC-процесс (idea 414)
