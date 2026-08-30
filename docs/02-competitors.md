# 🔍 Анализ конкурентов и лучшие идеи из других языков

> **Research draft.** Сведения о лицензиях, версиях, поддержке транспортов и коммерческих условиях быстро меняются. Перед публикацией нужны ссылки на первичные источники и дата проверки каждого утверждения. Формулировки ниже являются инженерной оценкой, а не юридическим заключением.

## 1. Конкуренты на C#/.NET

### MassTransit — «комбайн»
- **Сильное**: транспорты (RabbitMQ/ASB/Kafka/SQS), саги-стейт-машины (Automatonymous), тест-харнесс `ITestHarness`, request/response, топология exchange-ов из типов.
- **Ограничения**: API и topology model требуют заметного обучения; актуальные условия лицензирования нужно проверять по официальному сайту для выбранной версии.
- **Берём**: тест-харнесс, топологию из иерархии типов, `ConsumeContext`, riders для Kafka.

### NServiceBus (Particular) — «энтерпрайз-стандарт»
- **Сильное**: recoverability (immediate + delayed retries), ServiceInsight/ServicePulse мониторинг, саги с timeouts, строгие best practices (commands vs events).
- **Слабое**: платный, тяжёлый, много legacy.
- **Берём**: политики восстановления, разделение команда/событие на уровне API, audit queue.

### Wolverine (JasperFx) — «самый современный»
- **Сильное**: хендлеры — чистые методы без интерфейсов, **codegen вместо рефлексии**, каскадные сообщения (возврат из хендлера = публикация), durable inbox/outbox, интеграция с Marten = полный Event Sourcing стек («Critter Stack»), локальные in-process очереди.
- **Слабое**: меньше транспортов, меньше комьюнити.
- **Берём почти всё**: это главный ориентир по DX.

### CAP (dotnetcore/CAP) — «Outbox как сервис»
- **Сильное**: простейший transactional outbox поверх твоей БД, дашборд, интеграция с EF/ADO.
- **Берём**: философию «БД пользователя = хранилище надёжности», дашборд.

### Rebus — «дружелюбный минимализм»
- **Сильное**: маленький API, `IHandleMessages<T>`, saga-хранилища, second-level retries.
- **Берём**: простоту конфигурации one-liner, полиморфную диспетчеризацию.

### Brighter + Darker — CQRS-диспетчер
- **Берём**: разделение Command Processor / Query Processor, Russian-doll middleware.

### Другие .NET
- **EasyNetQ**: простота API для RabbitMQ → берём «одно-строчный» pub/sub.
- **Silverback**: мощная Kafka-интеграция, chunking больших сообщений.
- **Dapr pub/sub**: sidecar-абстракция, CloudEvents по умолчанию → берём CloudEvents.
- **Orleans Streams**: виртуальные акторы + стримы → идея implicit subscriptions.
- **Akka.NET / Proto.Actor**: акторная модель, кластерный шардинг → идея partition-actor.
- **Marten**: Event Sourcing на PostgreSQL, async daemon проекций → база для AvtoBus.EventSourcing.

## 2. Java/JVM

### Axon Framework — эталон CQRS+ES
- **Берём**: `@CommandHandler`/`@EventSourcingHandler` на агрегате, upcasters для версий событий, tracking processors с token store, replay проекций, snapshot triggers.

### Spring Cloud Stream / Spring Modulith
- **Берём**: биндинги «канал → брокер» через конфиг, функциональная модель `Function<In,Out>`, Modulith externalized events (@ApplicationModuleListener → Kafka).

### Kafka Streams
- **Берём**: топология DSL (map/filter/join/window), state stores, EOS-транзакции → мини-DSL стрим-процессинга в AvtoBus.

### Vert.x EventBus / Micronaut / Quarkus (SmallRye Reactive Messaging)
- **Берём**: `@Incoming/@Outgoing` каналы-аннотации, компиляционный DI (Micronaut = аналог наших Source Generators).

## 3. Go

### Watermill — «роутер сообщений»
- **Берём**: Router + middleware (retry, poison queue, throttle, correlation), CQRS-компонент, Pub/Sub интерфейс из 2 методов.

### NATS / JetStream
- **Берём**: subject-иерархии с wildcard (`orders.*.created`), consumer-группы, KV Store поверх стрима.

### Temporal
- **Берём**: durable execution — сага как обычный код, который переживает рестарты; идея «workflow as code» для AvtoBus.Sagas.

## 4. Rust
- **Tokio + Tower**: `Service`/`Layer` — композиция middleware → наш `IBusMiddleware`.
- **rdkafka, lapin**: zero-copy подходы → `ReadOnlyMemory<byte>` в Envelope.

## 5. Elixir/Erlang

### Broadway
- **Берём**: конвейер stages: producers → processors → batchers; **back-pressure по demand**; батчинг с partition_by; graceful draining.

### Oban / Phoenix PubSub
- **Берём**: cron-джобы в БД, уникальные джобы (unique constraints) → идемпотентный enqueue.

## 6. JavaScript/TypeScript
- **NestJS microservices**: `@EventPattern/@MessagePattern` декораторы, транспорт-агностик.
- **BullMQ**: flows (родитель-дети джобы), rate limiting per group, sandboxed processors.
- **Berём**: flows → зависимые сообщения; rate limit per tenant.

## 7. Python
- **Celery**: canvas (chain/group/chord) — композиция задач.
- **FastStream**: автогенерация **AsyncAPI** документации из кода, DI в хендлеры, тестовый клиент.
- **Берём**: AsyncAPI-генератор — killer feature для AvtoBus.

## 8. Инфраструктурные эталоны
- **Kafka**: log compaction, consumer groups, транзакции.
- **RabbitMQ 4.x**: quorum queues, streams, at-least-once dead-lettering.
- **EventStoreDB (Kurrent)**: $by_category проекции, persistent subscriptions.
- **Pulsar**: multi-tenancy на уровне брокера, tiered storage.
- **Redpanda**: производительность без JVM, WASM-трансформации → идея встраиваемых трансформаций.

## 9. Итоговая матрица: что попадает в AvtoBus MVP

| Фича | Источник вдохновения | Приоритет |
|------|---------------------|-----------|
| Method-handlers + codegen | Wolverine, Micronaut | P0 |
| Transactional Outbox/Inbox | CAP, Wolverine | P0 |
| Middleware-пайплайн | Watermill, Tower, ASP.NET | P0 |
| Каскадные сообщения | Wolverine | P0 |
| Recoverability-политики | NServiceBus | P0 |
| Тест-харнесс | MassTransit | P0 |
| Саги durable-execution | Temporal, NServiceBus | P1 |
| Event Sourcing + проекции | Marten, Axon | P1 |
| Back-pressure/батчинг | Broadway | P1 |
| AsyncAPI автодок | FastStream | P1 |
| Дашборд | CAP, BullMQ (Taskforce) | P2 |
| Стрим-DSL | Kafka Streams | P2 |
