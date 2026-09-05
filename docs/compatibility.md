# Политика совместимости

## Wire-формат и подписи (E-03)

- Текущая версия исходящих подписей: **v3** (v2 + подписанная метка `avtobus-signed-at`).
- Принимаются входящие **v2 и v3** (`MinimumSignatureVersion = 2` по умолчанию).
- Правило: минорная версия **может добавить** новую wire-версию, но обязана
  **читать N-1 минимум два минорных релиза**. Breaking wire-изменения —
  только в мажоре (для preview-линейки 0.x — с явной пометкой `breaking для preview`
  в CHANGELOG, как было в 0.1.2).
- Запланировано: `avtobus-wire-version` в заголовке, golden-фикстуры конвертов
  прошлых версий в `tests/wire-fixtures/` (conformance «старые конверты читаются»).

## Схема БД outbox

- Текущая миграция: **v3** (партиционные лизы `avtobus_outbox_leases`).
- Relay обязан открывать схему v2 (read) и писать v3; downgrade ниже v2 не поддерживается.
- Запланированы тесты апгрейда схемы с v1.

## Пакеты

- Версии — только из MinVer-тега `v*`. До 1.0 публичный API может меняться в миноре;
  набор Stable-пакетов к 1.0: Abstractions, Core, InMemory, RabbitMq, Outbox.EfCore,
  Testing, Generators, Security (остальное — preview/experimental с пометками).
- `PackageValidation` + `PublicAPI.Shipped/Unshipped.txt` (E-11) — отложены до первой
  публикации: пакеты ещё ни разу не выкладывались на nuget.org, baseline для сравнения
  отсутствует. Включить сразу после релиза 0.2.
