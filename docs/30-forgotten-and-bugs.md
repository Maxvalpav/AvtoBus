# 🐞 Что забыл: пропуски + баги целостности

> **Исторический аудит эскизов.** Последующие файлы предлагают исправления, но ни один пункт не считается закрытым до переноса в `src/` и подтверждения тестом.

Второй, более глубокий аудит. Здесь не только «чего нет», но и **что написано неправильно** в уже существующем коде.

---

## 🔴 Часть A. Баги в уже написанном коде

Эти вещи выглядят готовыми, но **не работают** или сломают приложение.

### A1. DI lifetime: `DefaultBus` (Singleton) инжектит `IOutbox` (Scoped) — 💥 InvalidOperation
`DefaultBus` зарегистрирован как singleton, но `EfCoreOutbox` — scoped (нужен `DbContext`).
Singleton не может держать scoped-зависимость. **Приложение упадёт при старте** с `ValidateOnBuild`.
**Фикс:** `DefaultBus` берёт `IServiceScopeFactory` и резолвит outbox внутри scope текущего сообщения. См. `code/14`.

### A2. Reply-корреляция никогда не завершается — 💥 Request зависает навсегда
`DefaultBus.Request()` регистрирует `TaskCompletionSource` и ждёт, но **никто не вызывает `CompleteReply`**.
Нет middleware/консьюмера, который ловит ответное сообщение по `reply-to` и завершает TCS.
**Фикс:** `ReplyMiddleware` + временная reply-подписка. См. `code/14`.

### A3. `HasUnitOfWork` — фантом. Как вообще начать транзакцию?
`DefaultBus` проверяет `_accessor.Current?.HasUnitOfWork`, чтобы решить «в outbox или в брокер».
Но **никто и никогда не выставляет UoW**, и нет механизма привязки outbox к транзакции `DbContext`.
Значит outbox **никогда не сработает** из HTTP-контроллера (где нет `ConsumeContext`).
**Фикс:** `IMessageSession` / `UnitOfWork` scope + `SaveChangesInterceptor`, который сбрасывает буфер исходящих в ту же транзакцию. Это **сердце всей надёжности** — и оно было пропущено. См. `code/14`.

### A4. `editorconfig` объявляет `CA1848` (LoggerMessage) как **error**, но весь код — интерполяция
`_log.LogInformation($"...")` повсюду → с `CA1848=error` и `TreatWarningsAsErrors=true`
**проект не соберётся**. Либо генерировать `LoggerMessage`-делегаты, либо понизить правило.
**Фикс:** `code/14` — партиал-классы с `[LoggerMessage]`, либо `CA1848 = suggestion`.

### A5. `AddAvtoBus` в Core ссылается на `RabbitMqTransport`/`KafkaTransport`
`AvtoBusServiceCollectionExtensions.UseRabbitMq` живёт вроде бы в Core, но `RabbitMqTransport` — в отдельном пакете `AvtoBus.RabbitMq`. **Циклическая/невозможная зависимость.**
**Фикс:** `UseRabbitMq` должен быть extension-методом **в пакете `AvtoBus.RabbitMq`**, не в Core.

### A6. Двойная регистрация middleware
`TelemetryMiddleware` добавляется и в `ApplyDefaultPipeline` (в pipeline), и через `TryAddSingleton`, и ещё раз в `AddAvtoBus`. Плюс дубли `RecoverabilityMiddleware` в двух файлах с разной логикой. **Неопределённое поведение.**
**Фикс:** один источник правды — `code/14` наводит порядок.

### A7. `TestTransport` дважды создаёт `InMemoryTransport`, `Captured` может быть null
Конструктор `TestTransport(clock)` не инициализирует `Captured`, но методы пишут в `Captured?` — тесты «ловли сообщений» молча не работают. Плюс harness **не запускает pipeline вообще** — `DrainAsync` просто `Task.Delay`. Тесты бесполезны.
**Фикс:** `InFlightTracker` + событийный `DrainAsync` вместо `Task.Delay`, явный `host.StartAsync` перед первым `Publish`. См. `code/16-test-harness-fix.md`.

### A8. `ScopeMiddleware` создаёт scope, но `HandlerInvokerMiddleware` берёт `IBus` из `ctx.Services` — и снова каскадит через `bus.Publish`, минуя outbox scope
Каскадные сообщения из хендлера должны идти через **тот же** UoW/scope, иначе теряется транзакционность каскадов (A3). Сейчас — не идут.

### A9. `MethodDispatcher` резолвит `_declaringType` как `GetRequiredService` — но типы-хендлеры нигде не регистрируются для method-style
Для статических — ок. Для инстансных method-handler'ов класс не добавлен в DI → `InvalidOperation`.
**Фикс:** `ReflectionDispatcherBuilder` должен `AddScoped(declaringType)` и для method-style (сейчас только для интерфейсных).

### A10. `ConventionSubscriptionCatalog` использует `registry.All`, но `DispatcherRegistry` объявлял `All` как `partial` в другом файле — порядок сборки
Плюс `BusOptions` расширяется `partial`-ом в `code/11`, но исходный `BusOptions` в `code/01` **не `partial`** → не скомпилируется.
**Фикс:** пометить `partial` исходные `BusOptions`, `ConsumeContext`, `DispatcherRegistry`.

### A11. `AddGeneratedDispatchers` регистрирует статические классы-хендлеры в DI — 💥 InvalidOperation при `BuildServiceProvider`
Reflection-ветка пропускает статические типы (`if (!method.IsStatic)`), а генераторная ветка безусловно звала `TryAddConsumerService(handlerType)` → `AddScoped(staticClass)`, и приложение падало на старте (`Cannot instantiate implementation type '...OrderHandlers'`). Проявилось в smoke-прогоне `samples/AvtoBus.Logistics` (30 static-хендлеров).
**Фикс (реализован):** `BusConfigurator.TryAddConsumerService` теперь возвращается для `type.IsAbstract && type.IsSealed` (static). Проверено smoke-прогоном Runner.

---

## 🟠 Часть B. Забытые подсистемы (даже не в gap-анализе)

### B1. Транзакционный Unit of Work (см. A3) — **самое важное упущение**
Без него весь Outbox — декорация. Нужен `IMessageSession`, привязка к `DbContext.Database.CurrentTransaction`, сброс буфера в outbox при `SaveChanges`.

### B2. DLQ: хранилище + reader + replay
Везде «отправить в DLQ», но **нет кода**, который пишет rich-error-envelope, читает его, фильтрует и реплеит. Идеи 164–168 без реализации.

### B3. Local in-process queues (идея 15) — ✅ реализовано
`IBus.EnqueueLocal(message, queueName?, ct)` есть; добавлен процессор локальных очередей:
`AvtoBus.Core/Local/LocalQueueTransport.cs` — bounded-канал (`Channel`) с back-pressure (идея 353),
отложенной доставкой и DLQ (`{queue}.error`/`.poison`/`.expired`). Настройка — `bus.AddLocalQueue(name, capacity)`.
Обработка идёт штатным `ConsumerHost` с ретраями и каскадами. Заодно закрыт routing-гэп:
`ToQueue(...).Via(...)` раньше молча игнорировал транспорт (`RoutingTable.ResolveCore`). Тесты: `Local/LocalQueueTests.cs` (5).

### B4. Second-level retry (`IHandleFailed<T>`) — интерфейс есть, wiring нет
Кто оборачивает в `IFailed<T>` и вызывает fallback-хендлер? Никто.

### B5. Конфигурация через `appsettings.json` / `IOptions` — ✅ реализовано
Секция `"AvtoBus"` биндится через `AddAvtoBus(IConfiguration, configure?)` в
`AvtoBus.Core/Configuration/AvtoBusConfiguration.cs` (`Bind` + `ValidateOnStart`),
валидатор `AvtoBusConfigValidator` собирает все ошибки разом (идея 421).
Покрыты: ServiceName, DefaultTransport, лимиты контекста, чёрный список, канарейка,
аномалия-детектор, inbox, recoverability, локальные очереди. Тесты: `Configuration/ConfigurationTests.cs` (3).

### B6. OpenTelemetry setup-extension — ✅ реализовано
`AvtoBus.Core/Observability/OpenTelemetryExtensions.cs`:
`AddAvtoBusInstrumentation()` для `TracerProviderBuilder` (`AddSource(BusTelemetry.ActivitySourceName)`)
и для `MeterProviderBuilder` (`AddMeter(BusTelemetry.MeterName)`). Тесты: `Observability/OpenTelemetryExtensionTests.cs` (2).

### B7. Graceful shutdown / draining (идея 35)
`BusHost` не реализует drain: при остановке in-flight сообщения теряются. Нет `IHostApplicationLifetime` интеграции.

### B8. Идемпотентность реально не enforce-ится
`[Idempotent]` атрибут есть, `IInboxStore` интерфейс есть — но **EF-реализации `IInboxStore` нет**, и middleware её не использует по-настоящему.

### B9. `HandlerTimeout` не применяется
Атрибут есть, но никто не взводит `CancellationTokenSource.CancelAfter`.

### B10. Партиционированная обработка с порядком (идея 25) — нет кода
✅ **Реализовано.** `Consumer< T>().OrderedBy(keySelector, partitions)` уже существовал, но селектор был мёртвым кодом: `PartitionRouter` рутил только по `envelope.PartitionKey` или `MessageId`. Теперь:
- Роутер принимает `Func<ITransportMessage, string>? resolver`; `PartitionOf` = resolver → `envelope.PartitionKey` → `MessageId`.
- `ConsumerRunner.PartitionKeyResolver()` (ConsumerHost.cs) строит resolver: канонический ключ — `Envelope.PartitionKey` (проставлен на отправке `[PartitionKey]`/`SendOptions`), иначе ключ достаётся десериализацией тела через `options.Serializers.For(ContentType).Deserialize(body, messageType)` + селектор (receive-side `OrderedBy`); сбой десериализации не роняет доставку — фолбэк на `MessageId`.
- Тесты: `tests/AvtoBus.Tests/Reliability/PartitionOrderingTests.cs` (2): `PartitionedJob` без `[PartitionKey]` (путь receive-side селектора) и `AccountEvent` с `[PartitionKey]` (путь конверта). Порядок внутри ключа строго возрастает при 4 партициях и форсированном чередовании.

### B11. Сериализаторы MessagePack/Protobuf — только упомянуты
✅ **Реализовано.** `AvtoBus.Serialization.MessagePack` (`MessagePackBusSerializer`, content-type `application/x-msgpack`, LZ4-сжатие, contractless resolver) и `AvtoBus.Serialization.Protobuf` (`ProtobufBusSerializer`, content-type `application/x-protobuf`, `IMessage`-контракты, десериализация через статический `Parser` с фолбэком на `MergeFrom`). Экстеншены `UseMessagePack()` / `UseProtobuf()` регистрируют формат дефолтным; приём выбирает сериализатор по `Content-Type` конверта — один консьюмер принимает и JSON, и бинарные форматы. Тесты: `tests/AvtoBus.Tests/Serialization/BinarySerializerTests.cs` (6).

### B12. Migrations packaging для Outbox/ES/Scheduling
✅ **Реализовано (механизм + outbox).**
- Ядро (`src/AvtoBus.Core/Migrations/`): `ISchemaMigration` (ModuleName/Version/Sql), `ISchemaExecutor` (таблица версий + выполнение SQL), `SchemaMigrator : IHostedService` — применяет неприменённые миграции при старте хоста в порядке (module, version), идемпотентно (по таблице версий), forward-only без отката.
- API: `bus.AddSchemaMigration(migration)` — регистрирует миграцию и `SchemaMigrator` (один на хост, `TryAddEnumerable`).
- Outbox (`src/AvtoBus.Outbox.EfCore/Migrations/`): `EfSchemaExecutor<TDb>` — работает через соединение пользовательского DbContext (провайдер-нейтральные версии/upsert), `OutboxSchemaMigration` v1 — DDL `avtobus_outbox`/`avtobus_inbox` (PG-flavored, совпадает с `ConfigureOutbox`). `UseOutbox<TDb>()` регистрирует исполнитель + миграцию автоматически.
- Тесты: `tests/AvtoBus.Tests/Migrations/SchemaMigratorTests.cs` (5, на фейковом исполнителе: порядок, skip применённого, идемпотентность, частичная миграция, no-op) + `UseOutbox_ensures_module_schema_on_host_start` (PG, пропускается без PostgreSQL).

### B13. CloudEvents / AsyncAPI / Claim Check / компрессия
✅ **CloudEvents и Claim Check реализованы; AsyncAPI и компрессия — в бэклоге.**
- **CloudEvents 1.0** (идея 117): `bus.UseCloudEvents(source)` — исходящий конверт получает атрибуты `ce-specversion`/`ce-id`/`ce-type`/`ce-source`/`ce-time` (бинарный режим, совместимость с Knative/Dapr/Event Grid). Код: `src/AvtoBus.Core/Runtime/CloudEvents.cs`, применение в `EnvelopeFactory.Create`.
- **Claim Check** (идея 84/138): `bus.UseClaimCheck(thresholdBytes, store)` — тело крупнее порога уезжает в `IBlobStore` (дефолт — `InMemoryBlobStore`), в конверте остаются заголовки `avtobus.claim-check` + размер; на приёме `ClaimCheckService` разворачивает тело до десериализации. Код: `src/AvtoBus.Core/ClaimCheck/*`, встраивание в `AvtoBusClient` (исходящий) и `MessageProcessor` (входящий).
- **CLI dlq** (идеи 91/164/168): команды `list` / `replay` / `replay-all` / `delete` / `status` поверх `DlqReader` для любого транспорта, подключённого к процессу (по умолчанию in-memory). Код: `src/AvtoBus.Cli/DlqCommand.cs`.

---

## 🟡 Часть C. Проектные/процессные пропуски

| Что | Почему важно |
|---|---|
| `README.md` (корневой) с бейджами, quick start | Лицо проекта |
| `SECURITY.md` + threat model (STRIDE) | Обещано в идеях 496, 499 |
| `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md` | OSS-гигиена |
| `CHANGELOG.md` + политика версионирования | Обещано (идея 439) |
| `LICENSE` (MIT) | Юридически обязателен |
| ADR (Architecture Decision Records) | Идея 413 |
| Версионирование самого фреймворка (MinVer/Nerdbank) | Как считается версия из git |
| `build/icon.png` | Ссылается `Directory.Build.props` — иначе pack упадёт |
| Полифилы для генератора (netstandard2.0) | `IsExternalInit`, `[ModuleInitializer]` под ns2.0 |
| Troubleshooting / FAQ / Cookbook | DevEx |
| Migration guides (MassTransit/NServiceBus/Rebus) | Захват аудитории |
| Sample-проекты (реальный код, не сниппеты) | Есть `samples/AvtoBus.QuickStart`, `AvtoBus.AotSample*`, `AvtoBus.Logistics` (30 сервисов); CI-прогон Logistics — джоба `logistics-sample` в `ci.yml` |
| Performance tuning guide | Идея 379–380 |
| Локализация диагностик RU/EN | Идея 445 |

---

## ✅ Что чиню прямо сейчас (в этой итерации)

1. **`code/14-reliability-glue.md`** — Unit of Work + транзакционный outbox-binding (A1, A3, A8, B1), reply-корреляция (A2), DLQ store/reader/replay (B2), second-level retry (B4), local queues (B3), фикс DI lifetime.
2. **`code/15-hosting-config.md`** — конфиг через `IOptions`/appsettings (B5), OTel-extension (B6), graceful shutdown (B7), health-checks registration, HandlerTimeout (B9), migrations-hosted-service (B12), LoggerMessage (A4).
3. **`31-project-meta.md`** — README/SECURITY/CONTRIBUTING/CHANGELOG/LICENSE контент, полифилы, версионирование, ADR-шаблон (часть C).
4. **Бинарные сериализаторы (B11), CloudEvents и Claim Check (B13), CLI dlq (идеи 91/164/168)** — реализованы и покрыты тестами (см. B11/B13 выше).
5. **Партиционированная обработка (B10) и migrations packaging (B12)** — реализованы и покрыты тестами (см. B10/B12 выше).

Остальное (AsyncAPI, компрессия, ES/scheduling-миграции, sample-проекты, migration guides) — в бэклоге `27-gap-analysis.md`, добьём по запросу.
