# AvtoBus — публичная документация

> Статус продукта: **0.1.3 (preview)**. API может меняться в минорных версиях;
> уровни зрелости пакетов (Stable / Preview / Experimental) — в работе.
> Внутренние черновики и каталог идей в git не публикуются.

## С чего начать

- [README](../README.md) — 30-секундный быстрый старт, три сценария, таблица пакетов.
- [Getting Started](getting-started.md) — установка, первое сообщение, следующие шаги.
- [Какой транспорт выбрать](decision-guide.md) — decision guide.
- [Гарантии доставки — честно](guarantees.md) — at-least-once / exactly-once без маркетинга.
- [Outbox и Inbox](outbox.md) — транзакционная доставка и дедуп.
- [Безопасность](security.md) — подписи, шифрование, fail-fast в Production.
- [Наблюдаемость](observability.md) — метрики, трейсы, алерты.
- [Миграция между версиями](migration.md) — breaking-изменения и порядок обновления.
- [Совместимость](compatibility.md) — wire-формат, версии подписей, миграции outbox.
- [FAQ](faq.md) — частые вопросы.
- [CHANGELOG](../CHANGELOG.md) — что изменилось, включая `[Unreleased]`.
- [SECURITY](../SECURITY.md) — threat model, production-чеклист, приватный репорт.
- [CONTRIBUTING](../CONTRIBUTING.md) — как собрать, протестировать и прислать PR.

## Концепции в двух словах

- **Конверт** — сообщение + `CorrelationId`/`CausationId` + W3C `traceparent`.
  Один бизнес-поток — один идентификатор.
- **Пайплайн** — `IBusMiddleware`: дедуп, circuit breaker, recoverability
  (ретраи → DLQ), батчи, саги.
- **Outbox** — каскады отправляются только после коммита бизнес-транзакции;
  `PartitionKey` даёт FIFO на ключ при любом числе relay.
- **Inbox** — дедуп повторных доставок; хендлеры обязаны быть идемпотентными.
- **Подпись v3** — HMAC + метка времени (anti-replay, окно 5 мин).
