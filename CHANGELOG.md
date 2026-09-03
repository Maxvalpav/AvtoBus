# Changelog

Все заметные изменения AvtoBus. Формат — [Keep a Changelog](https://keepachangelog.com/ru/1.1.0),
версионирование — [SemVer](https://semver.org/lang/ru/).

## [Unreleased]

## [0.1.3] - 2026-09-03 — чинка релизного конвейера

### Исправлено

- `global.json`: точный пин SDK 10.0.302 (`rollForward: disable`) — lock-файлы
трескались на другом SDK (плавающая версия `Microsoft.NET.ILLink.Tasks`).
- Lock-файлы перегенерированы под финальный скоуп `IsAotCompatible`
(тесты/бенчмарки/семплы исключены); проверен `--locked-mode` restore на Linux.
- `SECURITY.md`: ссылка на gitignored `docs/` заменена текстом (ломала docs-check в CI).

## [0.1.2] - 2026-09-03 — аудит-харденинг: безопасность, порядок, воспроизводимость

### Безопасность (breaking для preview: wire-формат подписи)

- Подпись конвертов **v3**: подписанная метка `avtobus-signed-at`, окно валидности
`MaxSignatureAge` (5 мин) + `MaxClockSkew` (1 мин) — граница переигрывания (anti-replay).
Исходящие по умолчанию v3, входящие принимаются v2/v3 (`MinimumSignatureVersion = 2`).
- `KeyRing`: HKDF-SHA256 вместо PBKDF2 для мастер-секрета; допуск проверки на эпоху
вперёд (рассинхрон часов); `CurrentKeyEpoch` для диагностики.
- `SECURITY.md`: версии, приватный репорт, threat model, production-чеклист, ранбук ротации.

### Корректность доставки

- Outbox: партиционные лизы (`avtobus_outbox_leases`, миграция v3, `PartitionLeaseTtl`) —
FIFO per `PartitionKey` при любом числе relay (порядок фаз peek → acquire → claim);
head-of-line внутри ключа + ожидание подписчиков вместе с головой; бесключевые идут дальше.
- `ConsumerHost`: атомарный снапшот ранеров вместо живого `List` (гонка метрик/health на старте).
- Request/response слушает reply-очередь на **каждом** транспорте, а не только на default.
- `InboxDedupMiddleware`: игнор недоверенного заголовка `consumer`, типизированный
`InboxDedupMiddleware<TDbContext>`, классификация уникальных нарушений по SqlState/кодам
вместо текста ошибки.
- `ProductionDefaults()`: fail-fast при `MasterSecret`/`OutboundRatePerSecond` без security-пакета.

### Наблюдаемость и эксплуатация

- Метрика `avtobus.outbox.oldest_pending_age` + DB-backed `avtobus.outbox.pending`
(`IOutboxHealthProvider`, refresh на простое); Grafana-панели, алерт `AvtoBusOutboxOldestStuck`,
раздел runbook.
- `AvtoBusClient.CancelScheduledAsync` логирует несработавшие отмены вместо молчания.

### Сборка и supply chain

- `IsAotCompatible` для шиппаемых библиотек: trim/AOT-анализаторы — ворота сборки
(аннотации динамических фич, source-gen контексты для DTO сериализаторов, `dynamic` убран из Kafka).
- `packages.lock.json` + `--locked-mode` в CI; `global.json` — `latestPatch`;
MinVer-only версии (у taskVersionPrefix удалён); CI vulnerable-gate реально падает.
- `Directory.Build.props`: починка XML-комментария с `--` (ломал restore).

### Тесты

- Chaos на живом PostgreSQL: флэп транспорта (backoff → восстановление без потерь),
передача эстафеты упавшего relay, DB-backed pending/oldest, порядок двумя relay на одном ключе.
- Регрессия мультитранспортного request/response; метрики gauge-тестами.

## [0.1.1] - 2026-09-02 — DataProfile + readonly + helm

### Добавлено

- `BusOptions.DataProfile` / `BusConfigurator.UseDataProfile(Gdpr|Ru152Fz)` — `PiiMaskingEnabled=true` по умолчанию (идея 498).
- `BusOptions.IsReadOnly` / `UseReadOnly()` — блокировка исходящих в `AvtoBusClient` + подавление каскадов в `MessageProcessor`; файл `~/.config/avtobus/readonly` и `AVTOBUS_READONLY=1`, CLI `avtobus readonly on|off|status` (идея 497).
- Helm-чарт `build/deploy/helm/*`, Prometheus alerts `build/deploy/prometheus/alerts.yaml`, runbook `build/deploy/RUNBOOK.md`.

## [0.1.0] - 2026-09-02 — продакшен-готовность

### Добавлено

- **Интеграционные тесты Outbox/Inbox на реальном PostgreSQL** (M2, док 15/24) — `tests/AvtoBus.Tests/OutboxPostgresTests.cs` (6 тестов, Testcontainers `postgres:16-alpine`, env `AVTOBUS_PG_URL`):
  - атомарность бизнес-данных и outbox в одной транзакции (commit/rollback);
  - доставка pending-строки relay-ом в транспорт;
  - crash-recovery: осиротевший `ClaimedAt` (relay умер между claim и publish) пере-claim'ится по `StaleClaim`;
  - inbox-дедупликация повторной доставки одного `MessageId` на реальной БД;
  - SKIP LOCKED: два relay не заклеймляют одну строку (20 сообщений, ноль дублей).
  - Исправление `OutboxRelay`: claim-запрос учитывает строки с `ClaimedAt` старше `StaleClaim`.

- **RabbitMQ-транспорт (M2, идеи 61–62)** — новый пакет `AvtoBus.RabbitMq`:
  - `RabbitMqTransport` (ITransport): очередь → durable quorum-очередь (+DLQ), топик → stream-очередь
    (лог с retention, как Kafka); publisher confirms; auto-recovery; bounded-буфер + BasicQos.
  - `UseRabbitMq(configure)` — DI-расширение `BusConfigurator`, дефолтный транспорт `rabbitmq`.
  - `Reject(requeue)` — пере-публикация с инкрементом `DeliveryAttempt` до `DeliveryLimit`, дальше — DLQ.
  - `RabbitMqEnvelopeSerializer` — заголовки по стандарту шины (идея 495), читаемость любым AMQP-тулом.
  - Conformance-сьют `RabbitMqTransportConformanceTests` (8 тестов, env `AVTOBUS_RABBIT_URL`);
    все 8 проходят против реального брокера (RabbitMQ 4.x, Docker).

- **Шаблоны проектов (M2, идея 401)** — новый пакет `AvtoBus.Templates`:
  - `dotnet new avtobus-worker` — worker-сервис на `Microsoft.NET.Sdk.Worker` (Host + PeriodicTimer-пример).
  - `dotnet new avtobus-webapi` — минимальный Web API с `POST /orders`.
  - Параметр `--transport` (`inmemory` по умолчанию, `kafka`, `redis`): условная генерация
    `UseInMemory`/`UseKafka`/`UseRedis` и соответствующих PackageReference.
  - Контракты `ICommand`/`IEvent`, консьюмеры по конвенции (статический `Handle`), каскад `PublishAsync`.
  - Smoke-тесты (8): упаковка/установка в изолированный hive, инстанциация по всем транспортам,
    сборка сгенерированных проектов против локального NuGet-feed.

- **Метапакет `AvtoBus`** — `src/AvtoBus`: Core + InMemory + JSON; единая точка входа `dotnet add package AvtoBus`.

- **.NET Aspire интеграция (M2, идея 419)** — новый пакет `AvtoBus.Aspire`:
  - `AddAvtoBusRabbit(name)` — RabbitMQ-ресурс с management-плагином и persistent-lifetime.
  - `WithAvtoBus(rabbit, postgres?)` — подключение проекта: service discovery (`WithReference`) +
    env `AVTOBUS_TRANSPORT=rabbitmq`.
  - `WithAvtoBusPostgres(db)` — подключение PostgreSQL (Event Store/outbox), env `AVTOBUS_STORAGE=postgres`.
  - Тесты построения модели ресурсов без запуска контейнеров (3 smoke-теста).

- **CLI `avtobus` (M2, M5)** — новый пакет `AvtoBus.Cli` (dotnet tool):
  - `doctor` — диагностика окружения, ядра, конфига.
  - `contracts --assembly <dll> [--format table|json]` — сканирование контрактов (ICommand/IEvent)
    из сборки рефлексией, имена на проводе через `MessageTypeNaming`.
  - `es explain [--assembly <dll>] [--contract <тип>]` — объяснение ES-модели: события/команды,
    потоки, агрегаты, Decider; закрывает критерий M5 «`avtobus es explain` работает».
  - `config show` / `config set-connection` — `~/.config/avtobus/config.json`.
  - `dlq list` — просмотр dead-letter сообщений из файла (JSONL, env `AVTOBUS_DLQ_FILE`).
  - `completion zsh|bash|fish|powershell` — генерация shell-автодополнения.
  - `Program.Main` на `System.CommandLine` 2.0.11 (`Parse` + `InvokeAsync(new InvocationConfiguration(), ct)`).

- **Мини-DSL стрим-процессинга (M7, идея 289)** — в `AvtoBus.EventSourcing` (`Streaming/EventStream.cs`):
  - `store.Stream<T>()` — глобальный поток событий типа T поверх Event Store (идея 289).
  - `Window(Tumbling/Sliding)` — окна фиксированной длины/шага по global_seq.
  - `GroupBy(key)` + `Aggregate(sum)` → `Into(...)` — агрегация по группам с доставкой в проекцию/колбэк.
  - `RunAsync()` возвращает позицию чекпоинта; end-of-stream flush доставляет последнее окно.

- **Event Catalog (M7, идеи 137, 138)** — новый пакет `AvtoBus.EventCatalog`:
  - `EventCatalogGenerator` строит словарь доменных событий из модели шины: дерево сообщений,
    JSON-схемы, владельцы-хендлеры (`MessageOwner`), маршруты (очередь/топик).
  - `GenerateHtml()` — самодостаточный single-file HTML-сайт (без внешних зависимостей, XSS-эскейп);
    `GenerateJson()` — стабильный JSON с вложенной AsyncAPI-спецификацией для CI-диффа по PR (идея 138).
  - `AddAvtoBusEventCatalog(configure)` регистрирует генератор в DI.

- **AsyncAPI генератор (M7, идея 114)** — новый пакет `AvtoBus.AsyncApi`:
  - `AsyncApiGenerator` строит AsyncAPI 3.0 спецификацию из модели шины: хендлеры
    `DispatcherRegistry` + маршруты `RoutingTable` (очереди команд, топики событий) +
    JSON-схемы контрактов.
  - `AsyncApiInfo` — метаданные документа (title/version/description/servers);
    `AddAvtoBusAsyncApi(configure)` регистрирует генератор в DI.
  - Отдаётся по `GET /asyncapi.json` одним маппингом; кормит генераторы клиентов и Event Catalog.

- **Roslyn-анализаторы (M2)** — новый пакет `AvtoBus.Analyzers` (AVB001-060):
  - `PublishCommandAnalyzer` (AVB004/005) — команды через `Publish*`, события через `Send*`
    ловятся на этапе компиляции; code-fix `PublishCommandCodeFix` переключает метод.
  - `MutableContractAnalyzer` (AVB010/017/022) — контракты с сеттерами, `TenantId` в теле,
    god-события.
  - `NamingAnalyzer` (AVB060) — события в прошедшем времени.
  - Пакет упаковывается как `analyzers/dotnet/cs` — подключается автоматически.

- **Production Hardening M6 (идеи 459, 461–467, 473, 479)** — новый пакет `AvtoBus.Multitenancy` + интеграция в ядро:
  - `TenantContext` (AsyncLocal) в ядре: EnvelopeFactory подхватывает тенанта автоматически,
    MessageProcessor восстанавливает его на время обработки — каскады наследуют tenant (461).
  - `TenantOptions`/`TenantRegistry` — уровень изоляции A (общие очереди + фильтр) / B (очередь
    per-tenant) / C (namespace per-tenant), регион размещения данных и квоты тенанта (462, 464).
  - `TenantRateLimitMiddleware` — per-tenant квота входящего трафика с backpressure через
    defer, жирный тенант не вытесняет мелких (464, 459).
  - `RegionRouteGuard` (IRegionPolicy) — data-residency by construction: публикация данных
    тенанта вне его региона блокируется на исходящем пути (467).
  - Атрибуты `[Region("eu")]` и `[GeoReplicated]` — привязка контракта к региону и участие
    в cross-region репликации (467, 473).
  - DevOps: `build/deploy/Dockerfile.chiseled` (chiseled-образ, non-root, пробы 468),
    `build/k8s/keda-scaledobject.yaml` (масштабирование по глубине очереди, 472),
    `build/deploy/generate-sbom.ps1` (SBOM в релизы, 479).
  - Починены два флаки-теста: гонка на `List` в метриках пайплайна и гонка флага в
    `HandlerTimeoutTests` (заменены на потокобезопасные структуры / TaskCompletionSource).

- **Event Sourcing M5 (идеи 259–261, 264, 269, 287, 294)**:
  - `ProjectionManager` — реплей проекций одной командой (`RebuildAsync`), статусы/lag
    (`GetStatusAsync`), blue/green переключение через `IVersionedProjection`
    (`BuildVersionAsync`/`ActivateVersionAsync`/`DropVersionAsync`) (259, 260, 261, 294).
  - Crypto-shredding (264): `es.Encrypt(...)` → `SubjectDataProtection` — AES-256-GCM per-subject;
    «право на забвение» = `ISubjectKeyRing.Forget`; зашифрованные поля читаются как null.
  - GDPR-отчёт (287): `IGdprReportService.BuildReportAsync(subjectId)` — все события субъекта
    с флагом `PiiReadable`.
  - Интеграция с шиной (269): `PublishStoreEvents(streamType)` — `StoreEventSubscription`
    публикует события стора в `IBus` с сохранением Correlation/Causation/Tenant.
  - `IEventSerializer.ResolveType` — резолв CLR-типа по имени события.

- **Security-блок (идеи 451–456, 459)** — новый пакет `AvtoBus.Security` + интеграция в ядро:
  - Подпись конвертов HMAC-SHA256 (`avtobus-signature`/`avtobus-signed-by`) + envelope encryption
    AES-256-GCM (`avtobus-encryption-nonce`) через `EnvelopeSecurity` (451, 455).
  - Ротация ключей с поколениями `KeyRing` + hosted-сервис `SecurityKeyRotationService`,
    настройки `SecurityOptions` (`MasterSecret`, `RequireSignature`, `EncryptBody`,
    `KeyRotationInterval`, `OutboundRatePerSecond`) (452, 459).
  - Авторизация хендлеров `[BusAuthorize]` + `AuthorizationMiddleware`/`IAuthorizer`,
    отказ → `UnauthorizedMessageException` → DLQ без ретраев (453).
  - Проброс пользователя `PrincipalContext` + `PrincipalSerializer`, заголовок `avtobus-user` (454).
  - PII-маскирование `[PersonalData]` + `PiiMasker` в диагностике/DLQ (456).
  - Подключение: `AddAvtoBusSecurity()` / `bus.UseEnvelopeSecurity(...)`.
- **Observability-блок (идеи 301–350)**:
  - OTel-трейсинг и метрики по конвенциям messaging: `consume.duration`, `critical.time`,
    `publish/consume.bytes`, `pipeline.step.duration`, `canary.rtt`, `queue.depth`,
    `consumer.lag`, `outbox.pending`, `dlq.size` (301, 302, 303, 334).
  - Спан обработки живёт на все ретраи; решения recoverability — события трейса
    `avtobus.recoverability` (305).
  - Логи со скоупом `MessageId/CorrelationId/MessageType/Attempt` (306).
  - EventSource `AvtoBus-Diagnostics` для dotnet-trace/counters (331).
  - Аудит «кто послал»: заголовок `avtobus-initiator` через `InitiatorContext` (332).
  - `RateLimitedLogger` — подавление лог-шторма (335).
  - Канарейка `UseCanary` — живой end-to-end healthcheck (337).
  - `TrafficAnomalyDetector` — детекция всплесков/провалов частоты (314).
  - Лимиты контекста `UseHeaderLimits` — хопы/объём/количество заголовков (313).
  - Чёрный список на лету `UseBlacklist` — блокировка паттерна без рестарта (349).
- **Kafka-транспорт (идеи 57–60)** — новый пакет `AvtoBus.Kafka` поверх Confluent.Kafka:
  - `UseKafka` — подключение транспорта; очереди и топики — топики Kafka (очередь = одна группа,
    топик = копия каждой группе).
  - Envelope ↔ Kafka: метаданные — в заголовки (`avtobus-message-id`, `avtobus-message-type`, ...),
    тело — в value (идея 495-совместимый стандарт заголовков).
  - Exactly-once (57): транзакционный продюсер (`transactional.id`, idempotence, `read_committed`).
  - Partition key (58): `PartitionKey` → Kafka key → партиция; порядок внутри партиции (60).
  - Back-pressure (59): пауза/возобновление партиций при переполнении буфера невыполненных.
  - Reject(requeue) = пере-публикация с инкрементом `DeliveryAttempt` + коммит исходного оффсета.
  - Conformance-сьют `KafkaTransportConformanceTests` — прогон через `AVTOBUS_KAFKA_BOOTSTRAP` в CI.
- **NATS/JetStream-транспорт (идеи 63–64)** — новый пакет `AvtoBus.Nats` поверх NATS.Net 2.x:
  - `UseNats` — подключение транспорта; каждый destination — JetStream стрим (subject = имя).
  - Durable push-consumers с queue groups: подписчики одной группы делят сообщения, разные группы
    получают копии (как Kafka).
  - Back-pressure через `MaxAckPending` (идея 63): JetStream не шлёт сверх лимита неподтверждённых.
  - `Reject(requeue)` = `NakAsync` → JetStream пере-доставляет с нативным инкрементом `NumDelivered`;
    `Reject(без requeue)` = `AckTerminate`.
  - Envelope ↔ NATS: метаданные в заголовках (`avtobus-*`), тело в Data (идея 495-стандарт).
  - Conformance-сьют `NatsTransportConformanceTests` — прогон через `AVTOBUS_NATS_URL` в CI.
- **Redis Streams-транспорт (идея 65)** — новый пакет `AvtoBus.Redis` поверх StackExchange.Redis:
  - `UseRedis` — подключение транспорта; очередь и топик — оба Redis Streams: очередь делит
    сообщения одной consumer group, топик — каждая группа получает копию стрима.
  - Consumer groups + XREADGROUP с батчами (`BatchSize`); `Reject(requeue)` = пере-публикация
    с инкрементом `DeliveryAttempt` + XACK исходного.
  - XAUTOCLAIM-переподхват зависших сообщений упавших консьюмеров (`MinIdleTimeMs`,
    `ReclaimInterval`) — идея 65.
  - Envelope ↔ Redis Stream: метаданные — поля записи (`avtobus-*`), тело — поле `body` (base64).
  - Conformance-сьют `RedisTransportConformanceTests` — прогон через `AVTOBUS_REDIS_URL` в CI.
- **SQL-транспорт (идеи 66–67)** — новый пакет `AvtoBus.Sql` поверх Npgsql:
  - `UseSql` — подключение транспорта; очередь — таблица PostgreSQL, топик — базовая таблица
    + копия на каждую группу консьюмеров (fan-out по high-water mark).
  - Конкурентные читатели без блокировок: `FOR UPDATE SKIP LOCKED` с батчами (идея 66).
  - Мгновенное пробуждение через `LISTEN/NOTIFY` после INSERT — вместо безудержного поллинга (идея 67).
  - Зависшие сообщения: claim истекает через `ReclaimTimeout` — возврат в доставку.
  - Envelope ↔ BYTEA-блоб (компактный JSON, тело base64); `Reject(requeue)` инкрементирует `DeliveryAttempt`.
  - Conformance-сьют `SqlTransportConformanceTests` — прогон через `AVTOBUS_PG_URL` в CI.
- **Azure Service Bus-транспорт (идеи 61–62)** — новый пакет `AvtoBus.AzureServiceBus` поверх Azure.Messaging.ServiceBus:
  - `UseAzureServiceBus` — подключение транспорта; очередь — ServiceBus Queue, топик — Topic с подпиской на группу.
  - Сессии для строгого порядка (61): `PartitionKey` → `SessionId`; `RequireSessions` включает requires-session.
  - Отложенная доставка — нативный `ScheduledEnqueueTime`.
  - Lock renew (62): фоновая задача продлевает lock, пока хендлер работает (`MaxAutoLockRenewalDuration`).
  - Ack = CompleteMessage; Reject(requeue) = AbandonMessage (брокер инкрементит DeliveryCount); Reject(без requeue) = DeadLetter.
  - Conformance-сьют `AsbTransportConformanceTests` — прогон через `AVTOBUS_ASB_CONNECTION` в CI.

 ### Исправлено

- Флаки-тест `PipelineStepMetricTests`: ожидание конкретного шага вместо «любой сэмпл»
  (статический инструмент видит обработки параллельных харнессов).

 ### Прод-харднинг 2026-09-02 (коммит 896c535)

- **Allowlist + ITypeResolver**: `BusConfigurator.UseAllowlist()` → `MessageProcessor` fail-closed `Poison` без десериализации; `MessageRegistry : ITypeResolver`, `AllowlistResolver` в `AvtoBus.Security`.
- **ConsumerHost**: гарантированный settlement при падении `MessageProcessor` (fallback `Poison` → `Reject`), `volatile IsPaused`, `DrainAsync` без утечки семафора, `PartitionRouter` с `FNV-1a` stable hash + `Shutdown CTS`.
- **Envelope context**: `InitiatorContext`/`TenantContext` → стек `AsyncLocal<Stack>` (вложенные скоупы без потери).
- **BusConfigurator**: `TrySetDefaultTransport` (первый транспорт остаётся дефолтом), идемпотентные `UseCompression`/`UseClaimCheck`, `AllowedMessageTypes`/`TlsOptions`.
- **EnvelopeSecurity**: `ProtectOutboundAsync` + `RateLimiter` с jitter 0–30 ms (thundering herd).
- **Инфра**: `Dockerfile.chiseled` → `avtobus` (`AssemblyName=avtobus`), `docker-compose.dev` пины образов + `healthcheck` + NATS, `CI` concurrency + `postgres:17`/`kafka:3.8.0`, pack 31 lib + CLI (32 пакета).

## [0.1.0] - 2026-08-14 — ядро

Первоначальная реализация ядра: конверт, пайплайн, recoverability, транспорт InMemory,
outbox на EF Core, саги, source generator, тест-харнесс.
