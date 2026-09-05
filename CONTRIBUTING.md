# Contributing to AvtoBus

## Требования

- .NET 10 SDK ровно версии из `global.json` (`rollForward: disable` — так надо для
  детерминированных `packages.lock.json`; см. E-05 в аудите).
- Docker — для брокеров в conformance-тестах (RabbitMQ, PostgreSQL, Kafka, NATS, Redis).

## Быстрый старт

```bash
dotnet restore AvtoBus.slnx --locked-mode
dotnet build AvtoBus.slnx -c Release --no-restore
dotnet format AvtoBus.slnx --verify-no-changes --no-restore
```

Поднять брокеры локально:

```bash
docker compose -f build/docker-compose.dev.yml up -d
```

## Тесты

Без брокеров гоняются только unit-тесты; транспортные conformance-сьюты включаются
env-переменными (иначе — `Assert.Skip`), те же имена, что в CI:

```bash
# RabbitMQ + PostgreSQL (outbox/inbox, SQL-транспорт)
$env:AVTOBUS_RABBIT_URL = "amqp://guest:guest@localhost:5672/"
$env:AVTOBUS_PG_URL = "Host=localhost;Port=5432;Database=avtobus;Username=avtobus;Password=avtobus"
# Kafka / NATS / Redis
$env:AVTOBUS_KAFKA_BOOTSTRAP = "localhost:9092"
$env:AVTOBUS_NATS_URL = "nats://localhost:4222"
$env:AVTOBUS_REDIS_URL = "localhost:6379"

dotnet test AvtoBus.slnx -c Release --no-build
```

Только conformance одного транспорта:

```bash
dotnet test tests/AvtoBus.Tests -c Release --no-build --filter "FullyQualifiedName~TransportConformance"
```

## Pack-проверка (как в CI)

```bash
dotnet pack AvtoBus.slnx -c Release -o ./artifacts-verify
```

Тесты/семплы/бенчмарки исключены через `IsPackable=false` в `Directory.Build.props` —
вручную список пакетов перечислять не нужно.

## Стиль

- `dotnet format` — ворота в CI, прогоняй до пуша.
- Публичный API документируется XML-комментариями; новых `NoWarn` не добавляем
  (текущие `CS1591/CS1573/CS1574/CS1734` — долг, см. E-10).
- Коммиты: короткий префикс + описание (`fix:`, `feat:`, `audit:`, `chore:`, `docs:` …).
- Секреты (`MasterSecret` и т.п.) — только из конфигурации, никогда литералами
  в примерах и тестах вне `tests/`.

## Definition of Done для PR

1. Тесты (unit + при необходимости conformance) в том же PR.
2. Строка в `CHANGELOG.md` в разделе `[Unreleased]`.
3. Страница/раздел в публичной документации, если меняется публичное поведение.
4. Без новых `NoWarn`, `dotnet build` — 0 ошибок, `dotnet format` — чисто.

## Безопасность

Уязвимости — только приватно, см. `SECURITY.md`. Не открывай публичные issue
с деталями эксплойта.
