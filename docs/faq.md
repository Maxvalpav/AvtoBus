# FAQ

## Это production-ready?

Нет. Версия 0.1.x — preview: инфраструктура (CI, supply chain, conformance)
зрелая, но публичный API и часть пакетов ещё меняются. Набор Stable к 1.0:
Abstractions, Core, InMemory, RabbitMq, Outbox.EfCore, Testing, Generators, Security.

## Exactly-once есть?

Только для цепочки Kafka→Kafka в транзакциях. Всё остальное — at-least-once
+ идемпотентность на вашей стороне. Подробно — [guarantees](guarantees.md).

## Почему только .NET 10?

Фреймворк использует API .NET 10 и AOT-стек актуального SDK. Мультитаргет
`net8.0` для Abstractions/Core рассматривается (нужны `#if` вокруг новых API),
но пока не введён — LTS-пользователям придётся ждать или собирать из исходников.

## Нужен ли брокер для старта?

Нет: InMemory + файловые/памятные реализации закрывают монолит. Брокер понадобится
для персистентности между рестартами и горизонтального масштабирования —
см. [decision-guide](decision-guide.md).

## Где задать вопрос / сообщить о баге?

Issue-шаблоны в `.github/ISSUE_TEMPLATE` (bug/feature). Уязвимости — только
приватно по `SECURITY.md`, не в публичных issue.

## Как понять, куда делось сообщение?

Логи со скоупом `CorrelationId`, трейс `avtobus.recoverability`, DLQ-просмотр
в дашборде. CLI `avtobus trace --correlation` — в дорожной карте.
