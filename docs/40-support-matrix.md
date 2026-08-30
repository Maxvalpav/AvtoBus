# 📦 Матрица поддержки AvtoBus (TFM / RID / AOT)

> Статус: **pre-alpha, пакет не опубликован.** Матрица — план поддержки, а не обещание.
> После публикации каждая строка пересматривается на основе фактических CI-артефактов
> (release gate 6: raw CI artifacts по commit SHA) и сообщений об ошибках пользователей.

## 1. Целевые платформы (TFM)

| Пакет | .NET 10 | .NET 11 | Комментарий |
|---|---|---|---|
| `AvtoBus.Core` | ✔ (net10.0) | Планируется | Ядро: pipeline, envelope, routing, outbox-контракты |
| `AvtoBus.InMemory` | ✔ | Планируется | Транспорт для тестов и local development |
| `AvtoBus.RabbitMq` | ✔ | Планируется | |
| `AvtoBus.Sql` | ✔ | Планируется | SQL Server/PostgreSQL transport |
| `AvtoBus.Kafka` | ✔ | Планируется | Confluent.Kafka |
| `AvtoBus.Nats` | ✔ | Планируется | NATS JetStream |
| `AvtoBus.Redis` | ✔ | Планируется | Redis Streams |
| `AvtoBus.AzureServiceBus` | ✔ | Планируется | |
| `AvtoBus.Outbox.EfCore` | ✔ | Планируется | Требует EF Core 10 (net10.0) |
| `AvtoBus.Sagas` / `EventSourcing` / `Scheduling` | ✔ | Планируется | |
| `AvtoBus.Security` / `Multitenancy` / `Dashboard` | ✔ | Планируется | |
| `AvtoBus.AsyncApi` / `EventCatalog` | ✔ | Планируется | |
| `AvtoBus.Generators` / `AvtoBus.Analyzers` | ✔ (netstandard2.0) | ✔ (netstandard2.0) | Roslyn-компоненты, TFM не зависит от рантайма |
| `AvtoBus.Testing` / `AvtoBus.Aspire` / `AvtoBus` | ✔ | Планируется | |

- Политика: новый TFM добавляется, когда его SDK доступен в CI (канонический список версий — `global.json`).
- Multi-targeting (net10.0;net11.0) не вводится заранее: это удваивает CI-нагрузку без обратной связи с пользователями pre-alpha.

## 2. RID и ОС

| RID | JIT (портативный) | Native AOT | Статус |
|---|---|---|---|
| `win-x64`, `linux-x64`, `osx-x64` | ✔ | Проверен только `linux-x64` | CI job `aot`: publish `linux-x64` + smoke (Core + InMemory + RabbitMQ) |
| `win-arm64`, `linux-arm64`, `osx-arm64` | Планируется | Не проверен | Тестируется при публикации пакета |
| Прочие RID | Не проверено | Не проверено | — |

- AOT-статус перечислен для **пакетов целиком**, а не для отдельных типов: полнота trim-аннотаций проверяется публикацией sample с `TrimMode=full`, `SuppressTrimAnalysisWarnings=false`.
- Транспорты Kafka/NATS/Redis/ASB и Sql/Outbox.EfCore **не проходили** AOT-проверку: сгенерированные dispatcher-пути (source generator) — AOT-safe, но внешние клиенты (Confluent.Kafka, NATS.Client, Azure SDK, Npgsql) могут требовать trim-конфигурации. До подтверждения sample-ом эти пакеты помечаются «AOT TBD».

## 3. Компилятор и CI

| Компонент | Значение |
|---|---|
| .NET SDK | 10.0.x (см. `global.json`) |
| C# | latest, `LangVersion=latest`, nullable enable |
| OS CI | ubuntu-latest (build/test/aot/benchmarks/conformance), Windows и macOS — после публикации |
| Символы | snupkg + SourceLink (GitHub) при `ContinuousIntegrationBuild=true` (release) |
| Пакетный менеджер | NuGet.org (публикация только по тегу `v*`) |

## 4. Что не входит в поддержку pre-alpha

- .NET 8/9: не поддерживаются (требуются иные BCL/EF-контракты).
- Trimming/ReadyToRun для всех RID: только `linux-x64` sample-проверка.
- Windows service / macOS AOT: не проверено.

## 5. Ссылки

- Версионирование и публикация: `.github/workflows/release.yml` (тег `v*` → `/p:Version`).
- AOT-гейт: `.github/workflows/ci.yml` job `aot`.
- SLO производительности: `adr/0006-benchmark-slo-methodology.md`, `20-benchmarks.md`.
- Обновление этой матрицы обязательно при изменении `TargetFramework`/RID-покрытия.
