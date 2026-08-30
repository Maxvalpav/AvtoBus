# Release Readiness Board

> **Статус: Planning / execution gate.** Этот документ превращает спецификацию AvtoBus в порядок PR-ов. Никакой пункт не считается закрытым без ссылки на `src/`, тест и CI artifact.

## Цель

Свести все документы в один исполняемый маршрут от текущего состояния `design draft / pre-alpha` до первого MVP.

## Канонический порядок PR-ов

| PR | Название | Результат | Gate |
|---:|---|---|---|
| 001 | `bootstrap-solution` | `AvtoBus.sln`, `Directory.Build.props`, Core project | `dotnet build /warnaserror` |
| 002 | `core-contracts` | `Envelope`, `IBus`, `ConsumeContext`, `Route`, options | API snapshot tests |
| 003 | `inmemory-transport` | InMemory transport + ack/nack/redelivery | L0-L1 conformance |
| 004 | `handler-dispatch` | Source-generated method handlers | AVB001/AVB002 compile tests |
| 005 | `test-harness` | Real host harness + fake time + event-driven drain | deterministic tests |
| 006 | `outbox-inbox-postgres` | EF Core Outbox/Inbox + relay | failure matrix: rollback/crash |
| 007 | `rabbitmq-provider` | RabbitMQ transport | L0-L2 conformance |
| 008 | `observability-hosting` | OTel, health checks, graceful drain | trace publish→consume |
| 009 | `sample-orders` | Minimal ASP.NET sample | end-to-end smoke |
| 010 | `release-preview` | package metadata, changelog, docs sync | preview NuGet dry-run |

## Exit criteria для MVP preview

- `src/AvtoBus.Core` не зависит от EF Core, ASP.NET, RabbitMQ, Kafka.
- `AvtoBus.Core` + `AvtoBus.InMemory` публикуются Native AOT без warnings.
- `34-verification-matrix.md`: Core, Delivery, Outbox/Inbox и Observability P0 строки имеют `Pass`.
- `36-transport-conformance.md`: InMemory и RabbitMQ проходят L0-L2.
- `38-reference-scaffold.md`: sample запускается в clean CI runner.
- Все snippets в public docs либо помечены как pseudocode, либо проходят doc-test.
- `FINAL.md` обновлён: что стало Verified, что осталось Draft.

## Release labels

| Label | Значение |
|---|---|
| `area/core` | API, Envelope, pipeline, DI |
| `area/transport` | Transport provider и conformance |
| `area/reliability` | Outbox, Inbox, retry, DLQ |
| `area/docs` | Документация, policy, ADR |
| `gate/aot` | Native AOT blocking issue |
| `gate/conformance` | Provider не проходит suite |
| `gate/security` | Threat model / signing / redaction |
| `good-first-issue` | Малый scoped task без архитектурного риска |

## Команды локальной проверки

```bash
dotnet restore
dotnet build -c Release /warnaserror
dotnet test -c Release --filter "Category!=Integration"
dotnet test tests/AvtoBus.Transport.Conformance -c Release
dotnet publish samples/AvtoBus.Sample.Orders -c Release -r linux-x64 /p:PublishAot=true

# Сэмпл 30 логистических микросервисов (отдельное решение, не входит в AvtoBus.slnx)
dotnet build samples/AvtoBus.Logistics/AvtoBus.Logistics.slnx -c Release

# Smoke-прогон: Runner гоняет все 30 сервисов и печатает сводку (EOF завершает по Enter-стопу)
dotnet run --project samples/AvtoBus.Logistics/runner/AvtoBus.Logistics.Runner -c Release
```

## Команда для ревьюера

Перед merge любого PR reviewer отвечает на четыре вопроса:

1. Какой claim из `34-verification-matrix.md` закрывает PR?
2. Какой ADR или spec является owner решения?
3. Есть ли тест, который ломается без этого PR?
4. Не нарушена ли граница Core из ADR-0001?

Если хотя бы один ответ пустой — PR остаётся draft.

## Что не делать до MVP

- Не добавлять Kafka, Event Sourcing, Dashboard SPA, stream DSL и AI-функции.
- Не публиковать benchmark-числа без raw BenchmarkDotNet artifact.
- Не писать «production-ready».
- Не менять wire protocol без ADR.
- Не копировать code sketches механически в `src/` без API review.