# Статус AvtoBus

Дата ревизии: 2026. Этот файл является каноническим источником статуса.

## Краткий вывод

AvtoBus сейчас является **проработанной спецификацией и набором непроверенных C#-эскизов**. Это ещё не готовая библиотека, не NuGet-пакет и не production-ready фреймворк.

Сборка Vite подтверждает только корректность оболочки проекта. Она **не компилирует C# внутри Markdown**.

## Что действительно завершено

| Артефакт | Статус | Подтверждение |
|---|---|---|
| Архитектурная концепция | Готов черновик | `01-architecture.md` |
| Целевой API | Готова спецификация | `03-core-api.md` |
| Анализ конкурентов | Черновик исследования | `02-competitors.md`; требует проверки источников |
| Каталог 500 идей | Готов | `04-13-ideas-*.md` |
| Roadmap и целевые SLO | Готов план | `19-roadmap.md`, `20-benchmarks.md` |
| C#-эскизы подсистем | Написаны, не проверены | `docs/code/01-16` |
| Структура будущего solution | Спроектирована | `28-project-structure.md` |
| Глоссарий и аудит рисков | Готовы | `29-glossary.md`, `30-forgotten-and-bugs.md` |
| Ключевые решения | Proposed, не Accepted | `docs/adr/0001-0005` |
| Wire protocol и scope | Specification draft | `33-wire-protocol.md`, `35-mvp-scope.md` |
| Матрица доказательств | Готов quality gate | `34-verification-matrix.md` |
| Release readiness | Готов execution gate | `40-release-readiness.md` |
| Веб-оболочка документации | Собирается | Vite build |
| Сэмпл 30 микросервисов | Добавлен, собирается локально (0 предупреждений/ошибок, AVB003 подавлен как в benchmarks) | `samples/AvtoBus.Logistics` |
| Команды сэмпла | 40+ команд/событий: возврат заказа (Cancel/Release/Complete/Refund), операционка (ТО, перенос, reroute, смена) | `Contracts/` |
| Event-driven в сэмпле | Analytics слушает `OrderPlaced`, Notifications слушает `Delivered` (publish/subscribe) | `services/Analytics.Service`, `services/Notifications.Service` |
| Smoke-прогон сэмпла | Runner: 17 проверок сценариев (доставка с ретрая, платёж отклонён, возврат заказа, операционка, event-подписки), exit code 0/1 | `runner/Program.cs` |
| CI-прогон сэмпла | Джоба `logistics-sample` в `.github/workflows/ci.yml`: build + smoke-run Runner | `.github/workflows/ci.yml` |
| Баг: static-хендлер в DI | Исправлен в `BusConfigurator.TryAddConsumerService` (skip static) | `src/AvtoBus.Core` |
| Реальный solution + сборка | `dotnet build AvtoBus.slnx -c Release`: 0 предупреждений, 0 ошибок | `AvtoBus.slnx`, CI `.github/workflows/ci.yml` |
| Тесты шины | 292 passed, 0 failed, 60 skipped (интеграционные, требуют Docker/брокеров) | `tests/AvtoBus.Tests` |
| Локальные in-process очереди (B3) | `IBus.EnqueueLocal`, `bus.AddLocalQueue`, `LocalQueueTransport` (bounded-канал, back-pressure, DLQ, delayed) | `src/AvtoBus.Core/Local/LocalQueueTransport.cs`, `tests/AvtoBus.Tests/Local` |
| Routing `ToQueue(...).Via(...)` | Починено: транспорт из правил теперь доклеивается к явному назначению | `src/AvtoBus.Core/Configuration/RoutingTable.cs` |
| Конфигурация через IConfiguration/IOptions (B5) | `AddAvtoBus(IConfiguration)` + секция `"AvtoBus"`, `Bind` + `ValidateOnStart` (fail-fast) | `src/AvtoBus.Core/Configuration/AvtoBusConfiguration.cs`, `tests/.../Configuration` |
| OpenTelemetry-экстеншн (B6) | `AddAvtoBusInstrumentation()` для `TracerProviderBuilder`/`MeterProviderBuilder` | `src/AvtoBus.Core/Observability/OpenTelemetryExtensions.cs`, `tests/.../Observability` |
| Бинарные сериализаторы (B11) | `UseMessagePack()` (LZ4, contractless) и `UseProtobuf()` (`IMessage`-контракты); приём по `Content-Type` | `src/AvtoBus.Serialization.MessagePack`, `src/AvtoBus.Serialization.Protobuf`, `tests/.../Serialization` |
| CloudEvents 1.0 (идея 117) | `UseCloudEvents(source)` — ce-* атрибуты на исходящем конверте (Knative/Dapr/Event Grid) | `src/AvtoBus.Core/Runtime/CloudEvents.cs`, `tests/.../Observability` |
| Claim Check (идея 84/138) | `UseClaimCheck(thresholdBytes, store)` — большой payload в `IBlobStore`, в брокере ссылка; разворот на приёме до десериализации | `src/AvtoBus.Core/ClaimCheck/*`, `tests/.../Observability` |
| CLI dlq (идеи 91/164/168) | `list` / `replay` / `replay-all` / `delete` / `status` поверх `DlqReader` | `src/AvtoBus.Cli/DlqCommand.cs`, `tests/AvtoBus.Tests/Cli` |
| Партиционированная обработка (B10, идея 25) | `OrderedBy(keySelector, partitions)` — один ключ строго последователен, ключ из `[PartitionKey]` или receive-side селектора (десериализация в роутере) | `src/AvtoBus.Core/Runtime/ConsumerHost.cs`, `tests/.../Reliability/PartitionOrderingTests.cs` |
| Migrations packaging (B12) | `SchemaMigrator` (IHostedService) + `ISchemaMigration`/`ISchemaExecutor`; `UseOutbox<TDb>()` сам поднимает `avtobus_outbox`/`avtobus_inbox` при старте | `src/AvtoBus.Core/Migrations/*`, `src/AvtoBus.Outbox.EfCore/Migrations/*`, `tests/.../Migrations` |

## Чего пока нет

- Опубликованных NuGet-пакетов и подтверждённой совместимости с .NET 10/11.
- Production security review и эксплуатационного опыта.
- Интеграционных прогонов против реальных брокеров (RabbitMQ/Kafka/NATS/Azure SB) в обычном прогоне тестов — они требуют Docker и гоняются в CI (`tests` + Testcontainers).

> Пункты «реальный src/`, «сборка без ошибок» и «тесты» из прежней версии этого списка закрыты:
> solution, Core и 292 теста существуют и зелёные. Статус строк выше отражает текущее состояние репозитория.

## Следующий обязательный этап

1. Создать минимальный solution: Core, InMemory, Testing и один sample.
2. Реализовать spike для ADR-0001-0005 и перевести принятые решения в `Accepted`.
3. Перенести только согласованный API, не копировать Markdown-фрагменты механически.
4. Добиться `dotnet build /warnaserror` и unit tests.
5. Реализовать InMemory conformance tests.
6. Затем добавить EF Core Outbox и RabbitMQ с интеграционными тестами.
7. Только после этого переносить Sagas, Scheduling и Event Sourcing.

## Definition of Done для MVP

- [ ] Solution и проекты существуют в репозитории.
- [ ] Core, InMemory и Testing собираются без предупреждений.
- [ ] Quick-start sample компилируется и запускается.
- [ ] At-least-once, ack/nack, retry и cancellation покрыты тестами.
- [ ] Outbox проверен тестом «commit / rollback / crash before publish».
- [ ] Inbox проверен повторной доставкой одного `MessageId`.
- [ ] Graceful shutdown возвращает незавершённые сообщения.
- [x] OpenTelemetry trace проходит publish -> consume (тест `PublishConsumeTraceTests`, экстеншн `AddAvtoBusInstrumentation`).
- [ ] Native AOT sample публикуется без trim warnings.
- [ ] Обязательные строки `34-verification-matrix.md` имеют `Pass` и CI artifacts.

## Правило заявлений

Пока чеклист MVP не закрыт, документация использует формулировки **«целевой API»**, **«планируется»**, **«эскиз»** и **«не проверено»**, а не «полностью рабочий» или «production-ready».