# AvtoBus documentation changelog

Отслеживает изменения самой документации. AvtoBus код ещё не существует, но структура docs эволюционирует по мере ревью.

## v0.5 (текущий)

### Добавлено

- `15-advanced-patterns-and-deep-dive.md` — глубокие темы, которые были упомянуты, но недостаточно раскрыты: failure scenarios matrix (30+ сценариев: network/DB/application/external/lifecycle/data corruption/cascading), performance budgets (throughput/latency/memory targets с trade-offs), Native AOT в реальности (checklist, common problems, per-package status, build profile, testing), MassTransit+Marten миграция (saga state migration SQL, event store migration SQL, IDocumentSession equivalent), F# samples (handlers, sagas, discriminated unions), Grafana dashboard JSON (10 panels с alerts), advanced tenant routing (resolution strategies, isolation strategies, onboarding), outbox batching в production (adaptive sizing, partial failure handling, lock management, migration SQL), Mermaid sequence diagrams, дополнительные utility snippets.

## v0.4

### Добавлено

- `11-source-generators-and-diagnostics.md` — полное описание всех Roslyn source generators, что они делают, что генерируется (с примерами generated code), полный каталог diagnostics (40+ IDs), CodeFixProvider, AOT compliance, escape hatches для scenarios где generator не подходит.
- `12-testing-guide.md` — 6-уровневая testing pyramid для EDA: unit → component → integration → contract → e2e → chaos. Включает golden envelope tests, property-based tests, mutation testing, deterministic time для sagas/workflows, testing checklists.
- `13-migration-cookbook.md` — реальные side-by-side migration examples с кодом для MediatR, MassTransit, NServiceBus, CAP, Wolverine, Dapr. 4-phase migration strategy с rollback plan.
- `14-reference-sample-and-cookbook.md` — полный OrderShop reference app (7 сервисов) с Aspire orchestration, K8s deployment, KEDA autoscaling. 20 практических cookbook recipes от request-response до zero-downtime schema evolution. Runbooks для типичных incidents.

### Обновлено

- README получил секцию "Порядок чтения" для разных ролей (architect / developer / SRE / migration team / contributor).
- README получил dependency graph документов через Mermaid.

### Метрики

- 14 файлов, ~5500 строк технической документации.
- Полное покрытие: architecture, contracts, code generation, testing, ops, migration, samples.

## v0.3

### Добавлено

- `09-durability-store-contract.md` — SQL-схемы всех store'ов, состояния outbox, lock model, batch dispatch, idempotency key API, migration CLI, storage backend matrix, failure recovery.
- `10-observability-otel-chaos-backfill.md` — OpenTelemetry messaging semantic conventions (`messaging.*`), AvtoBus extensions (`avtobus.*`), span tree, W3C trace propagation, clock skew handling, chaos test matrix, backfill workflow, replay CLI.

### Обновлено

- `02-architecture.md`: durability обзор + schema versioning/codec negotiation + AvtoBus SemVer + AOT readiness matrix.
- `04-reliability-workflows.md`: idempotency key API отдельно от inbox, batch outbox dispatch, Kafka native transactions vs outbox.
- `05-roadmap.md`: benchmarks methodology, criteria for preview→stable.
- `07-operations-security-observability.md`: multi-tenant isolation strategies (single DB / RLS / schema-per-tenant / DB-per-tenant).

## v0.2

### Добавлено

- `06-additional-competitors.md` — DTFx, Durable Functions, Orleans, Akka.Persistence, KEDA, RabbitMQ Streams, Redpanda, NATS JetStream, gRPC, Confluent Schema Registry, AsyncAPI, CloudEvents, Debezium, Hangfire/Quartz, Pact, F# railway.
- `07-operations-security-observability.md` — full metrics catalog, Prometheus/Grafana templates, alerting, SLO/SLI, TLS/mTLS, per-message auth, dashboard RBAC, PII/GDPR/retention, Claim Check, graceful shutdown, KEDA, DR, capacity planning, production checklist.
- `08-decisions-antipatterns-faq.md` — Mermaid decision tree, 10 anti-patterns, 7 ADR, FAQ, glossary, sequence diagrams.

### Обновлено

- README получил comparison matrix и "когда использовать".

## v0.1

### Изначальный набор

- `README.md`
- `01-market-research.md`
- `02-architecture.md`
- `03-api-design.md`
- `04-reliability-workflows.md`
- `05-roadmap.md`

Покрывает: анализ конкурентов, ядро архитектуры, публичный API, reliability/workflows, roadmap с MVP planes.
