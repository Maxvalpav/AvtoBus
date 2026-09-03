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

- Никаких прод-секретов в исходниках и тестах. В `samples/` и CI встречаются
  только dev-значения (`guest:guest`, `Password=app`) для локального запуска —
  прод-ключи всегда из Key Vault / K8s secrets.
- Сообщения могут содержать PII — не логируйте тела по умолчанию (идея 124);
  поля с `[PersonalData]` маскируются в диагностике и DLQ (`PiiMasker`, идея 456).
- Подпись конвертов HMAC-SHA256 (схема v2, см. ниже) + envelope encryption AES-256-GCM
  реализованы в `AvtoBus.Security` (идеи 451, 452, 455): подключение через
  `bus.UseEnvelopeSecurity(...)` / `AddAvtoBusSecurity()`.
- Авторизация хендлеров через `[BusAuthorize]` + `AuthorizationMiddleware` (идея 453);
  отказ → `UnauthorizedMessageException` → DLQ без ретраев.
- Проброс пользователя через заголовок `avtobus-user`: доверяем ему только при
  валидной подписи конверта (`SignedPrincipalExtractor`, см. ниже) (идея 454).
- Threat model (STRIDE) — ниже.

## Threat Model (STRIDE, идея 496)

Граница доверия: транспорт (брокер/сеть) считается недоверенным. Всё, что проходит
границу — входящий конверт — проверяется до десериализации. Внутри процесса (обработка)
граница доверия кончается у хендлера: его код и его зависимости — ответственность приложения.

| Угроза | Вектор | Митигация в AvtoBus | Остаётся на пользователе |
|---|---|---|---|
| **Spoofing** | Подделка источника сообщения | Подпись HMAC-SHA256 (`avtobus-signature`, схема v2), проверка до десериализации; `RequireSignature` делает её обязательной | Распространение `MasterSecret`; идентичность подписанта `SigningIdentity` |
| **Tampering** | Изменение тела/заголовков в пути | v2 покрывает MessageId, MessageType, Body, ContentType, TenantId, CorrelationId, CausationId, ReplyTo, PartitionKey, Priority, DeliverAt, TTL, TraceParent и `avtobus-user`; правки ломают проверку → DLQ. Не покрыты: мутабельные при транспортировке поля (DeliveryAttempt, SentAt, Hops, exception-заголовки) и кастомные заголовки — критичное кладите в тело | Ротация ключей (`KeyRotationInterval`); `SignatureVersion = 1` только на время rollout в смешанном парке |
| **Repudiation** | Отказ от факта отправки | `SignedByHeader` фиксирует подписанта | Аудиторские журналы приложения |
| **Information Disclosure** | Чтение тела на транспорте | AES-256-GCM (`EncryptBody`); нонс в заголовке, целостность поверх подписи | Управление ключами (KMS/Key Vault в проде) |
| **Denial of Service** | Флуд входящими / буст исходящих | Poison без ретраев для невалидных; outbound rate limit (`OutboundRatePerSecond`) | Входящий rate limiting на транспорте; лимиты размера сообщений |
| **Elevation of Privilege** | Обработка сообщения без прав | `[BusAuthorize]` + `AuthorizationMiddleware`; при подключённой безопасности principal извлекается только из подписанного `avtobus-user` (`SignedPrincipalExtractor`, неподписанный → аноним → отказ); `UnauthorizedMessageException` → DLQ. Пустая policy запрещает при `FailClosed` | Источник principal (SSO); хендлеры без атрибута — осознанно без авторизации |

Реализовано в 0.1.0–0.1.1 (прод):
- mTLS — НЕ поддерживается транспортами: заданный `SecurityOptions.Tls` бросает
  исключение на старте (fail-fast вместо молчаливого игнора). TLS termination —
  на стороне брокера/транспорта (матрица ниже).
- Allowlist типов — `BusConfigurator.UseAllowlist()` + `ITypeResolver`/`AllowlistResolver`, `MessageProcessor` → `Poison` без десериализации (идея 457, 451).
- Multi-tenancy — `AvtoBus.Multitenancy` (уровни A/B/C, `TenantRateLimitMiddleware`, `RegionRouteGuard` с атрибутами `[Region]/[GeoReplicated]`) (461–467, 473).
- Крипто-шреддинг — `AvtoBus.Security.CryptoShreddingService` (KMS `DeleteKeyAsync` + tombstone) + EventSourcing `SubjectDataProtection` per-subject AES-GCM (492–494); GDPR — `GdprSubjectIndexMigration` + `IGdprReportService` (287).
- PII-маскирование — `PiiMasker` для `[PersonalData]` уже в DLQ/диагностике (456):
  маска 128-битная детерминированная (SHA256(salt || value)) для корреляции;
  соль развёртки — `BusOptions.PiiMaskSalt` (дефолт встроенный, кросс-процессный).
  Покрывает только помеченные контракты: немаркированные типы в диагностике идут
  как есть — размечайте PII-поля. Короткие PII принципиально брутфорсятся при
  известной соли: соль — секрет, логи с масками — sensitive.
- Профили данных — `BusConfigurator.UseDataProfile(DataProfile.Gdpr|Ru152Fz)` включает `PiiMaskingEnabled` по умолчанию (идея 498).
- Аварийный режим — `BusConfigurator.UseReadOnly()` + `AvtoBusClient`/`MessageProcessor` блокировка исходящих, файл `~/.config/avtobus/readonly` и `AVTOBUS_READONLY=1`, CLI `avtobus readonly on|off|status` (идея 497).

## Безопасность в development

По умолчанию (без явного `MasterSecret`) `AddAvtoBusSecurity` использует
`avtobus-development-only` — этого достаточно для локальной разработки и тестов,
но **никогда не используйте его в проде**. В проде ключи должны приходить
из Key Vault / K8s secrets и ротироваться через `KeyRotationInterval`.

## mTLS: матрица поддержки

| Транспорт | mTLS на стороне AvtoBus | Как защищаться |
|---|---|---|
| InMemory | неприменимо (внутри процесса) | — |
| RabbitMQ / Kafka / NATS / Redis / Sql / AzureServiceBus | не поддерживается | TLS termination средствами брокера и клиента транспорта (connection string / опции клиента), сеть уровня VPC/ Private Link |

`SecurityOptions.Tls` оставлен как точка расширения, но его задание сейчас
бросает `InvalidOperationException` при старте: невыполненное обещание защиты
хуже честного «не умею».

## CLI и дашборд: принятые решения

- `avtobus config show` (table/json) маскирует пароли/токены в connection string;
  полное значение — только `config show-secret`. Файл конфига — `0600` на Unix.
- Сканирование сборок (`contracts`, `asyncapi`, `es`, `doctor --assembly`) грузит
  DLL только с локального диска (`.dll`/`.exe`, без URL) в collectible
  `AssemblyLoadContext` (файл не лочится, default-контекст не пачкается).
- `asyncapi --output` не перезаписывает существующий файл без `--force`.
- Дашборд требует authorization policy (`DashboardOptions.PolicyName`) на все
  endpoint-ы; опасные действия в проде запрещены без явного флага (идея 482).
  Просмотр DLQ санитизируется (`SanitizeBrowse`, дефолт вкл): redact заголовков
  `avtobus-user`/`avtobus-exception-stack`, best-effort маскирование PII-полей
  в JSON-телах, обрезка тел свыше `MaxBodyPreviewBytes`; фильтр тенанта —
  `DashboardOptions.TenantId`. Реплей перечитывает очередь заново, поэтому
  отображаемая копия может отличаться от оригинала.
