# AvtoBus — High-Throughput и Native AOT Optimization Guide

> **Статус: Specification draft.** Практическое руководство по получению максимальной производительности (100k+ msg/sec) и совместимости с .NET Native AOT.

---

## 1. Zero-Allocation Принципы на Горячем Пути (Hot Path)

Для достижения максимального прохода сообщений без нагрузки на Garbage Collector (GC), AvtoBus соблюдает следующие правила на горячем пути:

```
Receive Bytes ──> MemoryPool<byte> ──> Utf8JsonReader ──> Generated Dispatch ──> Zero-Alloc Handlers
```

### Правила и практики:
1. **Пул Буферов (`MemoryPool<byte>`):**
   Тело конверта не копируется в `byte[]`, а передаётся через `ReadOnlyMemory<byte>`, выделенный из `ArrayPool<byte>.Shared`.
2. **`ValueTask` вместо `Task`:**
   Все асинхронные методы шины (`Publish`, `Send`, `InvokeAsync`, `DispatchAsync`) возвращают `ValueTask`, избегая аллокаций при синхронном завершении.
3. **Отсутствие Boxing:**
   Все структурные типы и контексты передаются по ссылке.
4. **Замороженные Словари (`FrozenDictionary`):**
   Таблицы роутинга и заголовки конвертов после инициализации запечатываются через `ToFrozenDictionary()`, превращая `Lookup` в O(1) операцию без аллокаций и итераторов.

---

## 2. Настройка Native AOT (.NET 10/11)

AvtoBus полностью совместим с Native AOT (`dotnet publish -r linux-x64 -c Release /p:PublishAot=true`).

### 2.1 Исключение рефлексии
Source Generator `AvtoBus.Generators` генерирует весь код диспетчеризации и сериализации на этапе компиляции:

```csharp
// Генерируемый код JSON-контекста без рефлексии
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(PlaceOrder))]
[JsonSerializable(typeof(OrderPlaced))]
internal partial class AvtoBusJsonContext : JsonSerializerContext { }
```

### 2.2 Разрешение Trimming & AOT Warnings

В `Directory.Build.props` включается строгий анализ:

```xml
<PropertyGroup>
  <IsAotCompatible>true</IsAotCompatible>
  <EnableTrimAnalyzer>true</EnableTrimAnalyzer>
  <EnableSingleFileAnalyzer>true</EnableSingleFileAnalyzer>
</PropertyGroup>
```

Все сторонние библиотеки (например, `Npgsql` или `RabbitMQ.Client`) должны поддерживать AOT-аннотации (`[RequiresUnreferencedCode]`).

---

## 3. Рецепты Продакшн-Тюнинга Производительности

### 3.1 Тюнинг RabbitMQ для высокой пропускной способности

```csharp
builder.Services.AddAvtoBus(bus =>
{
    bus.UseRabbitMq(r =>
    {
        r.ConnectionString = "amqp://localhost";
        r.PrefetchCount = 128; // Большой prefetch для плотного потока
        r.UseQuorumQueues();
        r.ConsumerDispatchConcurrency = Environment.ProcessorCount; // По воркеру на ядро CPU
    });
});
```

### 3.2 Тюнинг Outbox Relay на PostgreSQL

Для быстрой вычитки Outbox используются параметры:

```json
{
  "AvtoBus": {
    "Outbox": {
      "BatchSize": 500,
      "Parallelism": 16,
      "PollIntervalMs": 500
    }
  }
}
```

В сочетании с индексом:
```sql
CREATE INDEX CONCURRENTLY ix_outbox_pending
ON avtobus_outbox (send_after)
WHERE sent_at IS NULL;
```

---

## 4. Сравнение Профилей Производительности

| Параметр | Low Latency Profile (Трейдинг / Игры) | High Throughput Profile (ETL / Аналитика) |
|---|---|---|
| `PrefetchCount` | 1 .. 4 | 256 .. 512 |
| `LingerMs` (Kafka / Confirm) | 0 ms | 10 .. 20 ms (батчинг) |
| `GC Mode` | `SustainedLowLatency` | `Server GC` |
| `Batch Size` | 1 | 500 .. 1000 |
| `Confirm Mode` | Sync confirm on each msg | Group async confirm |
| **Целевой показатель** | **p99 < 2 мс** | **> 100,000 msg/sec** |
