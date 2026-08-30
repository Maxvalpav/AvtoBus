# AvtoBus — Модель угроз STRIDE и Спецификация безопасности

> **Статус: Specification draft.** Определяет векторы атак, границы доверия и требования к безопасности для AvtoBus.

---

## 1. Границы доверия (Trust Boundaries)

```
[ HTTP / Public API ] ──(1)──> [ Application Worker ] ──(2)──> [ Database (Outbox) ]
                                      │
                                     (3)
                                      ▼
                             [ Broker / Transport ]
                                      │
                                     (4)
                                      ▼
                             [ Consumer Worker ] ──(5)──> [ Downstream DB / API ]
```

### Границы:
1. **TB1 (External -> Worker):** Недоверенная сеть. HTTP-клиенты, сторонние webhooks.
2. **TB2 (Worker -> Database):** Доверенная внутренняя сеть. Доступ к outbox/inbox таблицам.
3. **TB3 (Worker -> Broker):** Доверенная/полудоверенная сеть (AMQP/Kafka/NATS).
4. **TB4 (Broker -> Consumer):** Сеть брокера. Подписчик может быть в другом сервисе/контейнере.
5. **TB5 (Consumer -> Storage):** Локальное хранилище данных консьюмера.

---

## 2. Анализ угроз по методологии STRIDE

| STRIDE | Угроза | Граница | Механизм защиты в AvtoBus |
|---|---|---|---|
| **S**poofing | Подмена отправителя сообщения | TB3, TB4 | Обязательный заголовок `avb-signer` + HMAC-SHA256/Ed25519 подпись (ADR-0003, `33-wire-protocol.md`). |
| **T**ampering | Модификация тела или заголовков в брокере/сети | TB3, TB4 | Канонический хэш тела и заголовков с подписью. Модифицированное сообщение попадает в `poison` DLQ. |
| **R**epudiation | Отказ от факта отправки или выполнения действия | TB3, TB4, TB5 | Неизменяемый журнал аудита в Outbox + W3C Baggage (`avb-initiator`, `avb-user-id`) сквозь все вызовы. |
| **I**nformation Disclosure | Утечка PII или чувствительных данных из брокера/логов | TB1-TB5 | `[PersonalData]` атрибут -> автоматическое маскирование в логах/дашборде + AES-GCM шифрование тела. |
| **D**enial of Service | Затопление очереди (Poison message storm, retry loop) | TB1, TB4 | Retry budgets, `Poison` DLQ isolation, circuit breaker per-consumer, rate limiting per-tenant. |
| **E**levation of Privilege | Выполнение неавторизованной команды через подмену типа | TB1, TB4 | Strict allowlist десериализации (никаких `$type` с произвольными CLR-классами), `[BusAuthorize]` политики. |

---

## 3. Спецификация защиты от десериализационных атак

### Требование
Сериализатор **никогда** не должен загружать произвольные CLR-типы по имени из конверта.

### Механизм (`AllowlistTypeResolver`)
1. Все допустимые типы контрактов собираются при компиляции через Source Generator.
2. При получении конверта `message_type` проверяется по **замороженному словарю (FrozenDictionary)**.
3. Если тип не найден в allowlist -> мгновенный перевод в `poison` DLQ без попыток десериализации.

```csharp
// AvtoBus.Security/AllowlistResolver.cs
public sealed class AllowlistResolver : ITypeResolver
{
    private readonly FrozenDictionary<string, Type> _allowedTypes;

    public AllowlistResolver(IEnumerable<IMessageDispatcher> dispatchers)
    {
        _allowedTypes = dispatchers
            .ToDictionary(d => d.MessageType, d => d.ClrType, StringComparer.OrdinalIgnoreCase)
            .ToFrozenDictionary();
    }

    public Type Resolve(string messageType)
    {
        if (_allowedTypes.TryGetValue(messageType, out var type))
            return type;

        throw new SecurityException($"Message type '{messageType}' is not in the compile-time allowlist.");
    }
}
```

---

## 4. Спецификация маскирования персональных данных (PII Redaction)

### Атрибут `[PersonalData]`
Разработчик помечает чувствительные свойства в контрактах:

```csharp
public sealed record UserRegistered(
    Guid UserId,
    [property: PersonalData] string Email,
    [property: PersonalData] string PhoneNumber,
    DateTime RegisteredAt
) : IEvent;
```

### Поведение системы:
1. **Логирование:** `PiiRedactor` подменяет значения на `"***REDACTED***"` при форматировании JSON в `ILogger`.
2. **Дашборд:** Оператор без роли `SecurityAdmin` видит маскированные значения.
3. **Audit Trail:** Журнал аудита хранит либо маскированное тело, либо шифрует PII отдельным ключом.

---

## 5. Спецификация шифрования конверта (Envelope Encryption)

Для требований строгой конфиденциальности (финансы, медицина) тело сообщения шифруется симметричным ключом данных (DEK), а сам DEK зашифровывается мастер-ключом KMS (KEK).

```text
Plaintext Body ──[ AES-256-GCM (DEK) ]──> Ciphertext
                                                │
DEK ──[ KMS Encrypt (KEK) ]──> Encrypted DEK ───┴──> Wire Payload
```

### Структура зашифрованного конверта:
```json
{
  "avb-encryption": "aes-256-gcm",
  "avb-key-id": "kms-key-2026-v1",
  "avb-encrypted-dek": "base64...",
  "avb-nonce": "base64...",
  "avb-tag": "base64..."
}
```

 При удалении ключа тенанта в KMS (Crypto-shredding) все его исторические сообщения в брокере и Event Store становятся нечитаемыми — выполнение требований GDPR "Right to be Forgotten".

---

## 6. Мультитенантная изоляция (Multi-Tenant Security)

1. **Propagation:** `avb-tenant-id` протаскивается во всех каскадных сообщениях автоматически через `ConsumeContext`.
2. **Storage Isolation:** Запросы к Outbox/Inbox и Event Store содержат `WHERE tenant_id = @tenantId`.
3. **Cross-Tenant Prevention:** Анализатор Roslyn `AVB031` ругается, если handler пытается обратиться к данным другого тенанта без явного `[CrossTenantAccess]`.

---

## 7. Безопасность дашборда и CLI

1. **Дашборд:**
   - Обязательная аутентификация через ASP.NET Core Authentication/Authorization.
   - По умолчанию отключены опасные операции (`AllowDangerousOperationsInProduction = false`).
   - Операции Replay/Edit/Pause требуют отдельной политики `AvtoBusAdmin`.
2. **CLI:**
   - Требует явно переданного connection string или файла конфигурации с правами `0600`.
   - Не выводит пароли и секреты в stdout при `--verbose`.
