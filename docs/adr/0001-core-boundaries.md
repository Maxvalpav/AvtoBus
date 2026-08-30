# ADR-0001: Границы пакета AvtoBus.Core

- Статус: Proposed
- Дата: 2026
- Область: архитектура пакетов

## Контекст

Ранние эскизы регистрируют RabbitMQ, Kafka, EF Core, dashboard и hosting из одного extension-класса. Это создаёт циклические зависимости, мешает Native AOT и делает Core зависимым от инфраструктуры.

## Решение

`AvtoBus.Core` содержит только:

- публичные контракты `IBus`, `ConsumeContext`, `Envelope`;
- transport-neutral интерфейсы;
- pipeline и dispatch contracts;
- сериализацию JSON через BCL;
- InMemory transport как отдельный пакет `AvtoBus.InMemory`;
- diagnostics contracts на `ActivitySource` и `Meter`.

Core не ссылается на:

- ASP.NET Core;
- EF Core, ADO.NET providers и Npgsql;
- RabbitMQ, Kafka, NATS и другие broker clients;
- Blazor, dashboard и CLI;
- конкретный persistence provider.

Каждая интеграция владеет своим extension-методом:

```csharp
// AvtoBus.RabbitMq
public static BusOptions UseRabbitMq(this BusOptions options, Action<RabbitMqOptions> configure);

// AvtoBus.Outbox.EfCore
public static BusOptions UseEfCoreOutbox<TDbContext>(this BusOptions options)
    where TDbContext : DbContext;
```

## Разрешённый граф зависимостей

```text
Application
  -> AvtoBus.Core
  -> AvtoBus.RabbitMq -> AvtoBus.Core
  -> AvtoBus.Outbox.EfCore -> AvtoBus.Core
  -> AvtoBus.AspNetCore -> AvtoBus.Core

AvtoBus.Core -> BCL + Microsoft.Extensions abstractions only
```

## Последствия

Положительные:

- Core можно собирать и тестировать без брокера и БД.
- Транспорт не протекает в доменные handlers.
- Снижается trim/AOT surface.
- Провайдеры могут версионироваться независимо.

Отрицательные:

- Метапакет `AvtoBus` нужен только как удобная зависимость, но не как место реализации.
- Extension-методы распределены по пакетам.
- Conformance contracts должны быть вынесены в отдельный `AvtoBus.Transport.Abstractions` или оставлены в Core.

## Проверка решения

- Architecture test запрещает ссылки Core на `Microsoft.EntityFrameworkCore`, `RabbitMQ.Client`, `Confluent.Kafka`, `Microsoft.AspNetCore`.
- `dotnet list AvtoBus.Core package --include-transitive` проверяется allowlist-ом.
- AOT sample с Core + InMemory публикуется без trim warnings.

## Отклонённые варианты

1. Один монолитный пакет. Проще старт, но сильная связанность и большой dependency graph.
2. Service locator внутри Core для всех providers. Скрывает зависимости и усложняет проверку lifetime.