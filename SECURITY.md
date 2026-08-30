# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.1.x   | ✅ (active development) |

## Reporting a Vulnerability

AvtoBus ещё не достиг v1.0 и не используется в продакшене; тем не менее,
уязвимости безопасности важны сразу, а не после релиза.

Пожалуйста, НЕ публикуйте проблемы безопасности в публичном issue-трекере.

Вместо этого используйте GitHub Security Advisories.
Ответ в течение 72 часов. Если уязвимость критическая, вам ответят раньше.

Что указать в отчёте:

1. Затронутые версии (commit/tag).
2. Шаги воспроизведения (минимальный пример кода или скрипт).
3. Ожидаемое поведение и фактическое.
4. Возможное влияние (RCE, утечка данных, DoS).
5. Предлагаемый фикс (если есть).

После подтверждения мы выпускаем фикс в течение 7 дней для критических проблем.

## Безопасность по дизайну

- Никаких секретов в исходниках и тестах.
- Сообщения могут содержать PII — не логируйте тела по умолчанию (идея 124);
  поля с `[PersonalData]` маскируются в диагностике и DLQ (`PiiMasker`, идея 456).
- Подпись конвертов HMAC-SHA256 + envelope encryption AES-256-GCM реализованы
  в `AvtoBus.Security` (идеи 451, 452, 455): подключение через
  `bus.UseEnvelopeSecurity(...)` / `AddAvtoBusSecurity()`. См. `docs/code/17-security-observability.md`.
- Авторизация хендлеров через `[BusAuthorize]` + `AuthorizationMiddleware` (идея 453);
  отказ → `UnauthorizedMessageException` → DLQ без ретраев.
- Проброс пользователя через подписанный заголовок `avtobus-user` (идея 454).
- Threat model (STRIDE) — ниже.

## Threat Model (STRIDE, идея 496)

Граница доверия: транспорт (брокер/сеть) считается недоверенным. Всё, что проходит
границу — входящий конверт — проверяется до десериализации. Внутри процесса (обработка)
граница доверия кончается у хендлера: его код и его зависимости — ответственность приложения.

| Угроза | Вектор | Митигация в AvtoBus | Остаётся на пользователе |
|---|---|---|---|
| **Spoofing** | Подделка источника сообщения | Подпись HMAC-SHA256 (`avtobus-signature`), проверка до десериализации; `RequireSignature` делает её обязательной | Распространение `MasterSecret`; идентичность подписанта `SigningIdentity` |
| **Tampering** | Изменение тела/заголовков в пути | Подпись покрывает MessageId, MessageType, Body, ContentType и `avtobus-user`; любые правки ломают проверку → DLQ | Ротация ключей (`KeyRotationInterval`) |
| **Repudiation** | Отказ от факта отправки | `SignedByHeader` фиксирует подписанта | Аудиторские журналы приложения |
| **Information Disclosure** | Чтение тела на транспорте | AES-256-GCM (`EncryptBody`); нонс в заголовке, целостность поверх подписи | Управление ключами (KMS/Key Vault в проде) |
| **Denial of Service** | Флуд входящими / буст исходящих | Poison без ретраев для невалидных; outbound rate limit (`OutboundRatePerSecond`) | Входящий rate limiting на транспорте; лимиты размера сообщений |
| **Elevation of Privilege** | Обработка сообщения без прав | `[BusAuthorize]` + `AuthorizationMiddleware`; principal из подписанного `avtobus-user`; `UnauthorizedMessageException` → DLQ | Источник principal (IPrincipalExtractor/SSO) |

Остаточные области (не реализованы, в roadmap):
- mTLS для транспортов единообразно (`TlsOptions`, идея 452).
- Allowlist типов десериализации (идея 457) — сейчас неизвестный тип уходит в DLQ, явного allowlist-режима нет.
- Multi-tenancy изоляция и per-tenant ключи (идеи 461–467).
- Крипто-шреддинг + GDPR-отчёт в Event Sourcing (идеи 492–494).
- Профили данных `DataProfile.Ru152Fz` / `DataProfile.Gdpr` (идея 498).
- Аварийный режим «только чтение» `avtobus readonly on` (идея 497).

## Безопасность в development

По умолчанию (без явного `MasterSecret`) `AddAvtoBusSecurity` использует
`avtobus-development-only` — этого достаточно для локальной разработки и тестов,
но **никогда не используйте его в продакшене**. В проде ключи должны приходить
из Key Vault / K8s secrets и ротироваться через `KeyRotationInterval`.
