# Security Policy

## Поддерживаемые версии

| Версия | Поддержка |
| --- | --- |
| `1.x` | ✅ после первого стабильного релиза |
| `0.1.x` (текущая, preview) | ✅ best-effort: фиксы в ближайшем minor |
| `< 0.1` | ❌ |

До `1.0` проект в статусе preview: wire-формат конверта и API могут меняться
между minor-версиями, изменения фиксируются в [CHANGELOG.md](CHANGELOG.md).

## Сообщение об уязвимости

- GitHub → Security → **Report a vulnerability** (Private Vulnerability Reporting).
- Ответ в течение 72 часов, фикс security-исправлений — вне очереди релизов.
- Не открывайте публичные issues по уязвимостям до выхода фикса.

## Модель угроз (кратко)

Полная версия — `docs/36-threat-model.md` (внутренний каталог docs, в git не входит).

| Угроза | Митигация в AvtoBus |
| --- | --- |
| Подмена содержимого/маршрутизации | HMAC-SHA256, схема v3 покрывает тело, тип, tenant, correlation, ReplyTo, PartitionKey, Priority, DeliverAt, TTL, traceparent, CausationId |
| Downgrade подписи v2/v3 → v1 | `SecurityOptions.MinimumSignatureVersion = 2` по умолчанию, fail-closed |
| Переигрывание (replay) | v3 несёт подписанную метку `avtobus-signed-at`; старше `MaxSignatureAge` (5 мин) — отказ. Внутри окна дубли гасит inbox-дедуп / идемпотентный хендлер. Строгий nonce-reject осознанно НЕ используется: он убивал бы легитимные at-least-once ретраи |
| Рассинхронизация часов при ротации | Проверка допускает ключи на одну эпоху вперёд; подпись — на `MaxClockSkew` в будущее |
| Дешёвый ключ/пустой секрет | Fail-fast при старте вне Development; `PiiMaskSalt` по умолчанию — только для dev |
| Недоверенные типы при десериализации | Allowlist типов (`AllowlistOptions`, fail-closed) |
| Утечка PII в логи/DLQ/trace | Маскирование PII (`DataProfile`), соль развёртки |
| mTLS между сервисом и брокером | **Не** терминируется AvtoBus: настраивается на стороне брокера/транспорта. `SecurityOptions.Tls` бросает fail-fast с объяснением |

## Production-чеклист

1. `MasterSecret` — 256+ бит из Key Vault / KMS / K8s secrets, ротация по расписанию (`KeyRotationInterval`).
2. `RequireSignature = true`, исходящие — схема v3 (по умолчанию).
3. `MinimumSignatureVersion = 2` (по умолчанию) — не понижать без rollout-плана.
4. Своя `PiiMaskSalt` на развёртку (дефолт коррелируем между процессами).
5. Allowlist контрактов включён там, где типы приходят извне доверия.
6. `MaxSignatureAge` под сетевые реалии (5 мин по умолчанию; очереди с задержкой дольше окна — осознанное исключение, дубли ловит inbox).
7. TLS на брокере, доступ к DLQ/error-очередям ограничен, SBOM релиза проверен.

## Ротация ключей без остановки

1. Убедитесь, что все инстансы на версии с допуском эпохи вперёд.
2. Дождитесь смены эпохи (`KeyRotationInterval`).
3. Проверьте метрику/логи `SecurityViolation`: всплеск — часы какого-то инстанса ушли более чем на эпоху.
4. Старые поколения отмирают сами (`KeepPreviousKeyGenerations`, ключи затираются в памяти).
