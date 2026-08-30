# 💡 Идеи 151–200: Надёжность, Outbox/Inbox, ретраи, DLQ

### 151. Transactional Outbox одной строкой (CAP)
```csharp
bus.UseOutbox<AppDbContext>();
// В хендлере/контроллере:
await db.SaveChangesAsync(); // бизнес-данные + outbox-строки атомарно
```

### 152. Outbox push + polling fallback
После коммита — сигнал в `Channel<T>` (мгновенная отправка); поллинг раз в N сек подбирает пропущенное после падений.

### 153. Outbox-релей с претензией (claim) через `SKIP LOCKED`
Несколько реплик сервиса не дерутся за строки; зависшие claim-ы протухают по lease-таймауту.

### 154. Партиционированный outbox для порядка
Отправка строго по порядку в рамках `PartitionKey`; разные ключи — параллельно.

### 155. Outbox sharding по таблицам-суткам
`outbox_2025_06_01` — DROP TABLE вместо DELETE миллионов строк; ноль vacuum-боли PostgreSQL.

### 156. Inbox-дедупликация (exactly-once processing)
```sql
INSERT INTO inbox(message_id, consumer) VALUES(@id, @c) ON CONFLICT DO NOTHING;
-- 0 rows → дубликат → ack без обработки
```
Вставка в той же транзакции, что и бизнес-изменения хендлера.

### 157. Единая транзакция: inbox + handler + outbox
«Святой грааль» надёжности: приём отмечен, состояние изменено, исходящие записаны — атомарно; транспорт получает ack после коммита.

### 158. Дедуп-окно с Bloom-фильтром в памяти
Быстрый негативный ответ без похода в БД; БД — источник истины при положительном срабатывании.

### 159. Идемпотентность через `IdempotencyKey` бизнес-уровня
Ключ от клиента HTTP → протаскивается в конверт → результат кэшируется: повторный запрос вернёт тот же ответ, side-effects не повторятся.

### 160. Immediate + Delayed retries (NServiceBus)
```csharp
r.ImmediateRetries(3);                       // in-memory, мгновенно
r.DelayedRetries(5, Backoff.Exponential(5.Seconds(), jitter: true)); // через retry-очереди
```

### 161. Полликс-политики per-exception
```csharp
r.MapException<HttpRequestException>(RetryClass.Transient);
r.MapException<ValidationException>(RetryClass.Permanent);   // сразу poison
r.MapException<DbUpdateConcurrencyException>(RetryClass.Immediate);
```

### 162. Retry-бюджеты (Linkerd-идея)
Не более 20% трафика могут быть ретраями; при превышении — деградация в DLQ, защита от retry-штормов.

### 163. Circuit Breaker per-consumer
5 ошибок подряд → пауза консьюмера на 30с (сообщения остаются в брокере!) → half-open проба. Метрика + событие `BreakerOpened`.

### 164. Poison vs Error: две разные очереди
Не десериализовалось/нет типа → `*.poison` (навсегда, для разбора). Упало бизнес-исключением после ретраев → `*.error` (для реплея).

### 165. Rich error envelope
В error-очередь сообщение уходит с полным контекстом: stacktrace, host, версия сборки, attempt history, trace id — как NServiceBus + ServiceInsight.

### 166. DLQ-реплей с «исправлением»
```csharp
avtobus dlq edit <id> --set body.CustomerId=42 --replay
```
Патч тела перед повторной обработкой (аудит фиксируется).

### 167. Автоматический реплей после деплоя фикса
Правило: `orders.error` где `exception=NullReferenceException` и `version < 2.3.1` → авто-replay при выкате 2.3.1 (метка версии в error-конверте).

### 168. Rate-limited реплей
Реплей DLQ с ограничением N/сек, чтобы не устроить самим себе DDoS.

### 169. Вторая линия обороны: second-level retries (Rebus)
После исчерпания обычных ретраев сообщение оборачивается в `IFailed<T>` и может быть обработано специальным хендлером компенсации:
```csharp
public static Task Handle(IFailed<ChargeCard> failed, INotifier n)
    => n.AlertOps($"Оплата не прошла после всех попыток: {failed.ErrorDescription}");
```

### 170. Timeout сообщений на обработку
`[HandlerTimeout("00:00:30")]` → CancellationToken взводится, зависший хендлер прерывается, сообщение — в retry.

### 171. Heartbeat долгих хендлеров
`ctx.KeepAlive()` продлевает visibility/lock; вотчдог убивает молчащие обработки.

### 172. Дедлайн-распространение (gRPC deadline propagation)
`Envelope.Deadline` уменьшается по цепочке вызовов; хендлер не начнёт работу, если дедлайн уже прошёл.

### 173. Гарантированный порядок при ретраях
Ретрай сообщения с `PartitionKey` блокирует последующие того же ключа (опция `StrictOrdering`) — либо осознанный обгон (default).

### 174. Компенсационные транзакции first-class
```csharp
public static Compensation Handle(BookHotel cmd, IHotelApi api)
{
    var booking = api.Book(cmd);
    return Compensation.For(new CancelHotelBooking(booking.Id)); // регистрируется в saga-контексте
}
```

### 175. Записи о доставке: delivery receipts
Опция `RequestReceipt` — паблишер получает системное событие, когда все подписчики обработали (или провалили) событие.

### 176. Сквозной аудит: audit queue (NServiceBus)
Копия каждого обработанного сообщения (без тела или с телом) — в audit-стрим для комплаенса; ретеншн по политике.

### 177. Хаос-мидлварь для тестов надёжности
```csharp
p.UseChaos(c => { c.DuplicateProbability = 0.05; c.ReorderProbability = 0.1; c.DelayJitter = 2.Seconds(); });
```
Staging всегда живёт с хаосом — дубликаты и реордеринг перестают быть сюрпризом.

### 178. Проверка идемпотентности тестом
`IdempotencyVerifier.Verify(handler, msg)` — прогоняет хендлер дважды, сравнивает side-effects (моки) — в шаблоне тестов по умолчанию.

### 179. Ограничение конкуренции на ресурс
```csharp
bus.Consumer<GenerateReport>().SemaphorePerKey(m => m.CustomerId, max: 1);
```
Никаких гонок на одном агрегате без пессимистичных блокировок БД.

### 180. Optimistic concurrency ретрай-хелпер
`[RetryOnConcurrency(3)]` — перечитать агрегат и повторить хендлер при `DbUpdateConcurrencyException`.

### 181. Fencing tokens против зомби-обработчиков
Монотонный token в claim-е; сторадж отклоняет запись от процесса с устаревшим token-ом (защита от паузы GC/сети — идея Kleppmann).

### 182. Watchdog «застрявших» сообщений
Метрика возраста самого старого сообщения в очереди; алерт при превышении SLA; авто-эскалация приоритета.

### 183. Ретеншн и TTL по типу сообщения
`[Retention("7d")]` для событий в стриме; истёкшие удаляет брокер, не консьюмер.

### 184. Гарантия «не потеряли»: reconciliation job
Ночная сверка: `outbox.sent` vs `inbox.processed` по MessageId между сервисами; расхождения — в отчёт (идея из финтеха).

### 185. Двухфазный deploy контрактов
CLI-гейт: нельзя выкатить publisher новой версии, пока consumers не задеплоили поддержку (проверка через registry heartbeats).

### 186. Reliable timeouts переживают рестарты
`ctx.DeferAsync` пишет в durable-таблицу, не в память; после краша таймеры восстанавливаются.

### 187. Backoff с decorrelated jitter (AWS Architecture Blog)
`sleep = min(cap, random(base, prev*3))` — лучший против thundering herd; включён по умолчанию.

### 188. Приоритетный bypass для команд отмены
`CancelOrder` обгоняет очередь из 10k `PlaceOrder` через отдельную high-priority очередь и правило роутинга.

### 189. Гарантированная доставка в интеграции: webhook-retry шина
Исходящие webhooks партнёрам — через ту же машину ретраев/DLQ/дашборда: `bus.Webhook(url, payload, policy)`.

### 190. Обнаружение дубликатов на публикации
Паблишер-side дедуп: повторный `Publish` с тем же детерминированным MessageId в окне → no-op (журналируется).

### 191. Ограничение размера error-очереди
Quota + переливание старых в cold storage (S3) с индексом для поиска; error-очередь никогда не роняет брокер.

### 192. Автокарантин «пробивных» сообщений
Сообщение, уронившее консьюмер process-crash-ем (детект по маркеру начала обработки без маркера конца) → сразу poison, без повторного краша всей реплики.

### 193. Изоляция плохого тенанта
Ошибки только по `TenantId=X` → авто-выделение сообщений тенанта в карантинную очередь, остальные не страдают (bulkhead).

### 194. Retry-афинность реплики
Опция: ретрай направляется в ту же реплику (sticky), если хендлер прогрел локальный стейт; иначе — любой.

### 195. Журнал решений recoverability
Каждое решение (retry #, backoff, DLQ) пишется в Activity-события — в трейсе видна вся судьба сообщения.

### 196. Формальная верификация политики
`avtobus recoverability simulate --errors profile.json` — симуляция: сколько сообщений куда попадёт при заданном профиле ошибок, до продакшена.

### 197. Read-your-writes для UI
Хелпер: после `Send` API возвращает `CorrelationId`; SignalR-мост пушит клиенту завершение обработки — UI не поллит.

### 198. Сага-«качели» защита (ping-pong loop detection)
Детект циклов по CausationId-цепочке: глубина > N или повтор пары (type, key) → стоп + алерт (`AVB030` runtime).

### 199. Semantic ack: `Handled`, `Skipped`, `Superseded`
Хендлер сообщает исход; `Superseded` (пришло более новое состояние) — отдельная метрика, не считается ошибкой.

### 200. «Чёрный ящик» последней мили
Кольцевой буфер последних 1000 конвертов в памяти каждой реплики; при краше — дамп в файл; `avtobus blackbox read` для расследования.
