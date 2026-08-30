# 📄 Метафайлы проекта: README, SECURITY, LICENSE, полифилы

> **Статус: Templates.** Ссылки, бейджи и `github.com/avtobus/avtobus` — заглушки для организации, которая ещё не создана. Перед использованием замените на реальные URL, ключи подписи и контакты.

Содержимое обязательных корневых файлов, которые были забыты (часть C из `30-forgotten-and-bugs.md`).

---

## 1. README.md (корневой)

````markdown
# 🚌 AvtoBus

[![CI](https://github.com/avtobus/avtobus/actions/workflows/ci.yml/badge.svg)](https://github.com/avtobus/avtobus/actions)
[![NuGet](https://img.shields.io/nuget/v/AvtoBus.svg)](https://www.nuget.org/packages/AvtoBus)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Coverage](https://codecov.io/gh/avtobus/avtobus/branch/main/graph/badge.svg)](https://codecov.io/gh/avtobus/avtobus)

**AvtoBus** — современный EDA-фреймворк для ASP.NET Core 10/11.
Простой как Wolverine, надёжный как NServiceBus, с Event Sourcing как у Axon
и durable-сагами как у Temporal. Open source, AOT-ready, батарейки в комплекте.

## Установка

```bash
dotnet add package AvtoBus
dotnet add package AvtoBus.RabbitMq
dotnet add package AvtoBus.Outbox.EfCore
```

## Hello World

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.UseRabbitMq("amqp://localhost");
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
});

var app = builder.Build();
app.Run();

// Хендлер — просто метод
public static class OrderHandlers
{
    public static OrderPlaced Handle(PlaceOrder cmd, IOrderRepo repo)
    {
        var order = repo.Place(cmd.CustomerId, cmd.Items);
        return new OrderPlaced(order.Id);  // возврат = автопубликация
    }
}
```

## Почему AvtoBus?

| | AvtoBus | MassTransit | NServiceBus | Wolverine |
|---|:-:|:-:|:-:|:-:|
| Method-handlers | ✅ | ❌ | ❌ | ✅ |
| Native AOT | ✅ | ❌ | ❌ | 🟡 |
| Transactional Outbox | ✅ | ✅ | ✅ | ✅ |
| Event Sourcing | ✅ | ❌ | ❌ | ✅(Marten) |
| Durable Sagas | ✅ | 🟡 | ✅ | 🟡 |
| Dashboard | ✅ | ❌ | 💰 | ❌ |
| Open source | ✅ MIT | ✅ | 💰 | ✅ |

## Документация

- [Getting Started](docs/22-getting-started.md)
- [Architecture](docs/01-architecture.md)
- [500 идей](docs/README.md)
- [Migration с MassTransit](docs/30-migration-guides.md)

## Лицензия

MIT © AvtoBus Contributors
````

---

## 2. LICENSE

```
MIT License

Copyright (c) 2026 AvtoBus Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 3. SECURITY.md + Threat Model (STRIDE)

````markdown
# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x (LTS) | ✅ |
| 0.x | ❌ |

## Reporting a Vulnerability

**Не создавайте публичный issue для уязвимостей.**
Пишите на security@avtobus.dev или через GitHub Security Advisories.
Ответ в течение 48 часов, фикс в LTS — приоритетно.

## Threat Model (STRIDE)

| Угроза | Вектор | Митигация в AvtoBus |
|--------|--------|---------------------|
| **S**poofing | Поддельный отправитель сообщения | Подпись HMAC/Ed25519 (`AvtoBus.Security`), проверка `x-avb-key-id` |
| **T**ampering | Изменение тела в брокере | Подпись покрывает body+headers; envelope encryption |
| **R**epudiation | «Я это не отправлял» | Аудит `x-avb-initiator`, hash-chain событий в ES (идея 298) |
| **I**nformation disclosure | PII в логах/DLQ | `[PersonalData]` маскирование (124), crypto-shredding (264) |
| **D**enial of service | Флуд сообщений | Rate limit per-principal (459), retry budgets (162), bounded channels |
| **E**levation of privilege | Опасная десериализация | Allowlist типов (122, 457), запрет `TypeNameHandling.All` |

## Ответственность

| Покрывает AvtoBus | На стороне пользователя |
|-------------------|-------------------------|
| Подпись/шифрование сообщений | Управление ключами (KMS) |
| Allowlist десериализации | Валидация бизнес-данных |
| Изоляция тенантов (RLS) | Настройка политик доступа |
| mTLS к брокеру | Сетевая сегментация |
````

---

## 4. CONTRIBUTING.md

````markdown
# Contributing to AvtoBus

## Быстрый старт

```bash
git clone https://github.com/avtobus/avtobus
cd avtobus
docker compose -f build/docker-compose.dev.yml up -d
dotnet build
dotnet test --filter "Category!=Integration"
```

## Правила

1. **Все PR через fork + branch.** `main` защищён.
2. **Тесты обязательны.** Новый код — новые тесты. Покрытие ядра ≥ 80%.
3. **Бенчмарки не должны деградировать** > 5% throughput / 10% аллокаций.
4. **Analyzers зелёные.** `dotnet build /warnaserror` без ошибок.
5. **Doc-tests компилируются.** Сниппеты в docs/ проверяются в CI.
6. **Conformance-kit** для новых транспортов — обязателен.
7. **ADR** для архитектурных решений — в `docs/adr/`.

## Процесс фич

Крупные фичи — через RFC-issue с шаблоном (идея 414).
Обсуждение дизайна ДО кода.

## Commit convention

Conventional Commits: `feat:`, `fix:`, `perf:`, `docs:`, `test:`, `refactor:`.
Релизные ноты генерируются автоматически.
````

---

## 5. CHANGELOG.md (Keep a Changelog + SemVer)

````markdown
# Changelog

Формат: [Keep a Changelog](https://keepachangelog.com/), версионирование: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- (в разработке)

## [1.0.0] — 2026-XX-XX

### Added
- Ядро: IBus, ConsumeContext, middleware-пайплайн
- Транспорты: InMemory, RabbitMQ, Kafka
- Transactional Outbox/Inbox для EF Core
- Саги: state-based + durable execution
- Event Sourcing: PostgreSQL store, проекции, snapshots, upcasters
- Scheduling: cron, отложенные, leader election
- Source Generator (AOT-ready, zero-reflection)
- Dashboard, CLI, Test Harness

### Security
- Подпись сообщений, envelope encryption, PII-маскирование
````

### Политика версионирования (MinVer из git-тегов)

```xml
<!-- В Directory.Build.props -->
<ItemGroup>
  <PackageReference Include="MinVer" Version="6.0.0" PrivateAssets="All" />
</ItemGroup>
<PropertyGroup>
  <MinVerTagPrefix>v</MinVerTagPrefix>
  <MinVerDefaultPreReleaseIdentifiers>preview.0</MinVerDefaultPreReleaseIdentifiers>
</PropertyGroup>
```

Правила:
- **MAJOR** — breaking change публичного API (редко, только в новых LTS).
- **MINOR** — новые фичи, обратно совместимо.
- **PATCH** — багфиксы.
- **Breaking changes** между MAJOR запрещены; используем `[Obsolete]` минимум 1 minor до удаления.

---

## 6. Полифилы для Source Generator (netstandard2.0)

Генератор таргетит `netstandard2.0`, где нет `init`, `record`, `[ModuleInitializer]`.
Нужны полифилы (`build/polyfills/*.cs`):

### build/polyfills/IsExternalInit.cs

```csharp
// Позволяет использовать init-only свойства в netstandard2.0
namespace System.Runtime.CompilerServices
{
    using System.ComponentModel;

    [EditorBrowsable(EditorBrowsableState.Never)]
    internal static class IsExternalInit { }
}
```

### build/polyfills/ModuleInitializerAttribute.cs

```csharp
namespace System.Runtime.CompilerServices
{
    [AttributeUsage(AttributeTargets.Method, Inherited = false)]
    internal sealed class ModuleInitializerAttribute : Attribute { }
}
```

### build/polyfills/RequiredMemberAttribute.cs

```csharp
namespace System.Runtime.CompilerServices
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Field | AttributeTargets.Property)]
    internal sealed class RequiredMemberAttribute : Attribute { }

    [AttributeUsage(AttributeTargets.All, AllowMultiple = true)]
    internal sealed class CompilerFeatureRequiredAttribute(string featureName) : Attribute
    {
        public string FeatureName { get; } = featureName;
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(AttributeTargets.Constructor)]
    internal sealed class SetsRequiredMembersAttribute : Attribute { }
}
```

---

## 7. ADR (Architecture Decision Records)

### docs/adr/0001-source-generators-over-reflection.md

````markdown
# ADR-0001: Source Generators вместо рефлексии

**Статус:** Принято · **Дата:** 2026-01-15

## Контекст
Нужно диспетчеризировать сообщения к хендлерам. Классический путь (MassTransit,
Rebus) — рефлексия в рантайме. Проблемы: холодный старт, аллокации, несовместимость с AOT.

## Решение
Используем инкрементальный Source Generator для генерации диспетчеров на компиляции.
Рефлексия остаётся как fallback для динамических сценариев.

## Последствия
- ✅ Native AOT работает
- ✅ Ошибки роутинга на этапе сборки (AVB001–003)
- ✅ ~2–3x быстрее диспетчеризация
- ❌ Сложнее отладка генератора (митигируем snapshot-тестами)
- ❌ Генератор на netstandard2.0 (нужны полифилы)
````

### docs/adr/0002-outbox-in-user-database.md

````markdown
# ADR-0002: Outbox в БД пользователя

**Статус:** Принято · **Дата:** 2026-01-16

## Контекст
Проблема dual-write: сохранить бизнес-данные И отправить событие атомарно.

## Решение
Transactional Outbox в той же БД (подход CAP/Wolverine): исходящие пишутся
в outbox-таблицу в транзакции бизнес-данных. Relay отправляет асинхронно.

## Последствия
- ✅ Атомарность без распределённых транзакций
- ✅ Работает с любой БД (EF Core)
- ❌ Нужна БД (не подходит для stateless-only сервисов → есть режим без outbox)
- ❌ Двойная запись в БД (митигируем батчингом)
````

---

## 8. build/icon.png (заглушка)

`Directory.Build.props` ссылается на `build/icon.png` для NuGet.
Без файла `dotnet pack` упадёт. Нужен PNG 128×128 (логотип автобуса 🚌).
До готового логотипа — плейсхолдер, генерируемый в CI, или закоммиченный минимальный PNG.

```
build/icon.png   (128×128, < 50KB, PNG)
```

---

## 9. .gitignore (ключевое)

```gitignore
bin/
obj/
*.user
.vs/
.idea/
artifacts/
bench-*/
*.received.*        # Verify snapshot-тесты
TestResults/
coverage.*.xml
.DS_Store
```

---

## 10. Что ещё в бэклоге метафайлов

| Файл | Статус |
|------|--------|
| `CODE_OF_CONDUCT.md` | Contributor Covenant — скопировать шаблон |
| `.github/ISSUE_TEMPLATE/` | bug/feature/rfc шаблоны |
| `.github/PULL_REQUEST_TEMPLATE.md` | чек-лист PR |
| `.github/dependabot.yml` | автообновление зависимостей |
| `docs/adr/0003+` | остальные решения (сериализация, транспорт-контракт) |
| `samples/*/` | реальный код примеров (не только сниппеты) |
| `docs/30-migration-guides.md` | миграция с MassTransit/NServiceBus/Rebus |
| `docs/troubleshooting.md` | типовые проблемы и решения |
| `docs/cookbook.md` | рецепты «как сделать X» |
