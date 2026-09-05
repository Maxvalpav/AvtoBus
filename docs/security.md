# Безопасность

## Конверты

- **Подпись v3** (дефолт): HMAC-SHA256 + подписанная метка `avtobus-signed-at`,
  окно валидности 5 мин + допуск на часы 1 мин (anti-replay). Входящие принимаются
  v2/v3 (`MinimumSignatureVersion = 2`).
- **Шифрование тела**: AES-256-GCM (`EncryptBody`), ключи — HKDF-SHA256 от мастер-секрета.
- **Ротация**: поколения ключей `KeyRing` + `SecurityKeyRotationService`
  (`KeyRotationInterval`), допуск на эпоху вперёд при рассинхроне часов.

## Подключение

```csharp
services.AddAvtoBusSecurity(_ => { }); // + bus.UseEnvelopeSecurity(sec => { ... })
```

Секрет — из Key Vault / K8s secrets / конфигурации. Никогда литералом:
примеры с `"shared-secret"` из истории удалены именно поэтому.

## Авторизация и PII

- `[BusAuthorize]` на хендлере + `IAuthorizer`: отказ → DLQ без ретраев.
- `avtobus-user` пробрасывает principal; при включённой безопасности неподписанному
  заголовку не доверяем (fail-closed `SignedPrincipalExtractor`).
- `[PersonalData]` маскирует поля в диагностике и DLQ-просмотре.

## Fail-fast в Production

При `ASPNETCORE_ENVIRONMENT=Production` (или `DOTNET_ENVIRONMENT`) старт падает, если:

- `RequireSignature=false`;
- `MasterSecret` короче 32 символов или равен известному плейсхолдеру;
- дашборд замаплен без auth-политики (`PolicyName` пуст);
- дополнительно: in-memory как единственный транспорт — warning в лог.

Вне явного Production проверки не срабатывают (тесты и dev не трогаем).

## Threat model

Полная модель — в `SECURITY.md`: границы доверия, матрица mTLS по транспортам,
ранбук ротации ключей.
