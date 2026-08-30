# План доведения AvtoBus до работающего MVP

Этот документ заменяет устаревший gap-анализ. Файлы в `docs/code/` закрывают часть проектных вопросов, но не закрывают компиляцию и тестирование.

## P0: создать проверяемый продукт

| Шаг | Результат | Проверка |
|---:|---|---|
| 1 | `AvtoBus.sln`, Core, InMemory, Testing, sample | `dotnet build /warnaserror` |
| 2 | Единые contracts без дублей: `Envelope`, `Route`, options, context | API review + compile |
| 3 | Один корректный DI lifetime model | `ValidateScopes=true` |
| 4 | Один pipeline с документированным порядком middleware | unit tests порядка и short-circuit |
| 5 | Source-generated dispatch для minimal API | generator snapshot tests |
| 6 | InMemory transport с ack/nack/redelivery/back-pressure | conformance tests |
| 7 | Реальный TestHarness, который запускает host и ждёт события | deterministic tests, fake time |

## P1: надёжность и первый внешний транспорт

| Шаг | Результат | Проверка |
|---:|---|---|
| 8 | EF Core UoW + Outbox + Inbox | PostgreSQL Testcontainer, crash scenarios |
| 9 | Recoverability: immediate/delayed retry, poison/error DLQ | exception matrix tests |
| 10 | RabbitMQ transport | RabbitMQ conformance suite |
| 11 | Graceful drain, health checks, OpenTelemetry | host integration tests |
| 12 | Request/Response с временной reply endpoint | timeout, late reply, cancellation tests |
| 13 | Schema migrations и upgrade/rollback policy | migration tests from N-1 |

## P2: расширение платформы

| Модуль | Предусловие |
|---|---|
| Sagas | Inbox/Outbox и optimistic concurrency стабильны |
| Scheduling | Durable store, leader election и fake time проверены |
| Event Sourcing | Контракты, upcasting и projection checkpoints стабильны |
| Kafka | Общий conformance contract отделён от queue semantics |
| Security | Wire format и canonical signing representation зафиксированы |
| Multi-tenancy | Tenant propagation и storage isolation формализованы |
| Dashboard/CLI | Есть стабильные management API, а не прямой доступ к internals |

## Решения, которые ещё нужно принять

1. `IBus` singleton или scoped: определить доступ к UoW без captive dependencies.
2. Outbox API для HTTP: `IMessageSession`, interceptor или ambient scope.
3. Каскадный return: `Publish` всегда или вывод command/event по marker interface.
4. Reply routing: временная очередь, shared endpoint или transport-native inbox.
5. Единственный формат wire envelope и правила canonical signing.
6. Поведение при неизвестном типе: poison store, raw handler или compatibility mode.
7. Гарантии порядка при delayed retry.
8. Граница Core: никаких ссылок на EF Core, RabbitMQ, ASP.NET и Npgsql.

## Документационные задачи

- [x] Разделить specification, design draft и code sketch.
- [x] Убрать утверждение, что Markdown-код компилируется.
- [x] Назначить `FINAL.md` каноническим статусом.
- [ ] Проверить анализ конкурентов по первичным источникам и проставить дату проверки.
- [x] Добавить Proposed ADR по Core boundary, DI lifetime, Outbox transaction boundary, delivery semantics, handler contract и reply routing.
- [ ] Подтвердить ADR spike-реализацией и перевести выбранные решения в Accepted.
- [ ] После появления `src/` заменить snippets ссылками на реальные исходники.
- [ ] Включить doc-tests только для помеченных `compile`-блоков.

## Definition of Done

Актуальный чеклист находится в [FINAL.md](./FINAL.md). Документы `28-project-structure.md` и `31-project-meta.md` являются шаблонами, а не подтверждением наличия соответствующих файлов в репозитории.