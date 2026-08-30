# 💡 Идеи 101–150: Контракты, сериализация, версионирование

### 101. Контракты — `record` с init-only
Иммутабельность по умолчанию; анализатор ругается на мутабельные контракты (`AVB010`).

### 102. Строковый тип сообщения вместо CLR-имени
`orders.order-placed.v1` вместо `Contracts.OrderPlaced, Contracts` — переименование классов не ломает провод (проблема MassTransit решена).

### 103. `[MessageAlias]` и таблица алиасов
```csharp
[MessageAlias("orders.order-placed.v1", "legacy:OrderPlacedEvent")] // старое имя тоже понимаем
public record OrderPlaced(Guid OrderId);
```

### 104. System.Text.Json source-generated по умолчанию
`JsonSerializerContext` генерится AvtoBus.Generators для всех контрактов — AOT + скорость.

### 105. Плагины сериализации: MessagePack, Protobuf, Avro, MemoryPack
```csharp
bus.Serialization(s => { s.Default = Serializers.MessagePack; s.For("analytics.*").Use(Serializers.Avro); });
```

### 106. Content-type негоциация на приём
Один консьюмер принимает и JSON и MessagePack — по `ContentType` конверта; миграция форматов без флага дня.

### 107. Schema Registry интеграция (Confluent/Apicurio)
Регистрация схем при старте, проверка совместимости (BACKWARD/FORWARD) в CI.

### 108. Собственный лёгкий Schema Registry на PostgreSQL
`AvtoBus.SchemaRegistry` — таблица схем + REST; для тех, у кого нет Confluent.

### 109. Версионирование через upcasters (Axon)
```csharp
public sealed class OrderPlacedV1ToV2 : IUpcaster<OrderPlacedV1, OrderPlacedV2>
{
    public OrderPlacedV2 Upcast(OrderPlacedV1 old) => new(old.OrderId, old.Total, Currency: "RUB");
}
```
Цепочки v1→v2→v3; хендлеры пишутся только под последнюю версию.

### 110. Downcasters для обратной совместимости публикации
Пока живы старые подписчики, событие публикуется в двух версиях (double-publish window).

### 111. Weak-schema десериализация
Незнакомые поля игнорируются, отсутствующие — дефолтятся; строгий режим для банковских доменов: `[StrictContract]`.

### 112. Контракт-пакеты и `ContractsAssembly`
Соглашение: контракты в отдельной сборке без зависимостей; анализатор запрещает ссылки на доменные типы из контрактов (`AVB011`).

### 113. Генерация контрактов из AsyncAPI/OpenAPI
`avtobus contracts import asyncapi.yaml --out Contracts/` — типы + маппинги.

### 114. Генерация AsyncAPI из кода (FastStream)
`avtobus asyncapi export` — полная спека каналов/сообщений из compile-time модели; UI-документация из коробки.

### 115. Consumer-Driven Contract тесты (Pact-style)
```csharp
[Fact]
public Task OrderPlaced_matches_consumer_expectations() =>
    ContractVerifier.Verify<OrderPlaced>(against: "billing-service/pacts");
```
Ломающие изменения ловятся в CI паблишера.

### 116. Snapshot-тесты сериализации
Компайл-тайм фиксация wire-формата: изменение сериализации контракта без bump-а версии = красный тест.

### 117. CloudEvents 1.0 из коробки (Dapr)
`bus.UseCloudEvents()` — конверт мапится в атрибуты `ce-id`, `ce-type`, `ce-source`; совместимость с Knative/Dapr.

### 118. `[Obsolete]` для контрактов с телеметрией
Приём устаревшего сообщения → метрика `avtobus.contract.deprecated` + предупреждение в дашборде: видно, кто ещё шлёт старьё.

### 119. Реестр владения контрактами
`contracts.yaml`: владелец, SLA, каналы; CLI проверяет, что изменение контракта апрувит владелец (CODEOWNERS-стиль).

### 120. Автогенерация TypeScript/Java/Go клиентских типов
`avtobus contracts export --lang ts` — фронтенд получает типы событий для SignalR-моста.

### 121. Полиморфизм в JSON без type-hints по CLR-типам
Дискриминатор — стабильная строка: `"$type": "orders.item-discount.v1"` (никаких `Namespace.Class, Assembly` — безопасность).

### 122. Запрет опасной десериализации
Анализатор запрещает `TypeNameHandling.All`-паттерны; allowlist типов — единственный путь (урок уязвимостей .NET).

### 123. Валидация контрактов на публикации
FluentValidation/DataAnnotations прогоняются до отправки: мусор не попадает в брокер.

### 124. `Redacted<T>` — маскирование чувствительных полей
```csharp
public record UserRegistered(Guid Id, Redacted<string> Email);
```
В логи/дашборд — `***`, на провод — как есть (или шифрованно, идея 466).

### 125. Схемы совместимости в CI: `avtobus schema check`
Сравнение с прошлой версией из git: удаление поля/смена типа → fail сборки с понятным диффом.

### 126. Каноничный формат денег/дат в контрактах
Встроенные типы `Money`, `UtcInstant`, `DateOnly`-конвертеры; анализатор против `decimal Total` без валюты (`AVB015`).

### 127. Заголовки-конвенции как типизированный API
```csharp
ctx.Headers.IdempotencyKey; ctx.Headers.TenantId; ctx.Headers.Source;
```
Никаких магических строк в пользовательском коде.

### 128. Interning повторяющихся строк заголовков
`FrozenDictionary` + `string.Intern`-пул для значений с малой кардинальностью — минус аллокации.

### 129. Ленивая десериализация тела
`ConsumeContext.Message` материализуется при первом обращении; фильтр-middleware по заголовкам может отбросить сообщение бесплатно.

### 130. Частичная десериализация для роутинга
`Utf8JsonReader` вытаскивает только `PartitionKey`/`TenantId` без полного парсинга — для router-only узлов.

### 131. Бинарные метаданные схемы в заголовке
`schema-id: 42` (4 байта) вместо полного имени типа — экономия на Kafka с миллиардами сообщений (Confluent wire format).

### 132. Мультиформатные фикстуры совместимости
Репозиторий golden-файлов: одно событие в JSON/MsgPack/Avro/Proto; тест читает всеми десериализаторами всех поддерживаемых версий.

### 133. Автоматический `messageVersion` bump-помощник
`avtobus contracts bump OrderPlaced` — создаёт `OrderPlacedV3`, скелет upcaster-а и тест.

### 134. Контракт-первый workflow (schema-first опционально)
Proto/Avro-схемы — источник истины, C#-типы генерятся; для команд, где «схема — закон».

### 135. Валидация JSON Schema на границе
Для внешних партнёров: входящие сообщения валидируются JSON Schema до десериализации; отчёт об ошибках в poison-очередь с деталями.

### 136. Событие-конверт для нескольких арендаторов
`TenantId` — только в Envelope, никогда в теле; анализатор ловит `TenantId` в контрактах (`AVB017`) — предотвращает утечки между тенантами.

### 137. Словарь доменных событий (Event Catalog)
`avtobus catalog serve` — сайт с деревом событий, схемами, владельцами, графом «кто публикует/кто слушает» (вдохновение: eventcatalog.dev).

### 138. Публикация каталога как артефакта CI
Статический сайт каталога деплоится на Pages; PR показывает дифф графа событий.

### 139. Nullable reference types обязательны в контрактах
`#nullable enable` enforced анализатором; `string?` в контракте — осознанное решение с описанием.

### 140. Дефолты эволюции: добавляй поля, не переиспользуй
Правила Avro/Proto в аналитике: `AVB020` — переиспользование имени удалённого поля запрещено 2 версии.

### 141. Расширяемые метаданные: `Extensions` bag
```csharp
public IReadOnlyDictionary<string, JsonElement> Extensions { get; init; }
```
Неизвестные будущие поля переживают round-trip (forward-compat как в CloudEvents).

### 142. Идентичность события: детерминированный MessageId
`MessageId = Uuid5(namespace, businessKey)` — повторная публикация того же факта даёт тот же id → дедуп бесплатно.

### 143. `Sequence` в конверте для строгого порядка
Монотонный номер в рамках PartitionKey; консьюмер детектит пропуски и запрашивает переотправку (идея из FIX-протокола).

### 144. Семантические типы событий: Fact / Delta / Snapshot
`[Fact]` — свершившееся, `[Delta]` — изменение, `[Snapshot]` — полное состояние; дашборд и правила ретраев учитывают семантику.

### 145. Событие-снапшот с log compaction
`[Snapshot(Key = nameof(Sku))]` → Kafka compacted topic: новый подписчик получает только последние состояния.

### 146. Спецсообщения жизненного цикла
`ConsumerStarted`, `TopologyApplied`, `MessageDeadLettered` — сами являются событиями шины; можно подписаться и алертить.

### 147. Проверка размера контракта
`AVB021`: сериализованный размер типового экземпляра > 64KB — предупреждение, совет применить Claim Check.

### 148. Локализуемые описания контрактов
`/// <summary>` из XML-доков попадает в AsyncAPI/каталог; поддержка `ru`/`en` описаний.

### 149. Тестовые фабрики контрактов
Генератор создаёт `OrderPlacedFaker` (Bogus-совместимый) для каждого контракта — консистентные тестовые данные во всех сервисах.

### 150. Правило «одно событие — один факт»
Анализатор эвристик: событие с 30+ полями и именем `*Updated` → предложение расщепить (`AVB022`, learn-link на event storming).
