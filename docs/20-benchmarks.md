# 📊 Бенчмарки и целевые SLO

> **Все числа ниже являются целями или гипотезами, а не результатами измерений.** Они не должны использоваться в маркетинге до появления воспроизводимого benchmark-проекта и сырых артефактов CI.

## 1. Философия

- Планируем публиковать **воспроизводимые** бенчмарки: репо + Docker Compose + BenchmarkDotNet.
- Сравниваем себя с собой (регрессия) и с MassTransit/Wolverine/CAP на одном железе.
- **Не маркетинг** — все числа с оговорками (payload, топология, железо).

## 2. Целевые SLO ядра

| Сценарий | SLO | Комментарий |
|----------|-----|-------------|
| Publish → InMemory → Handler (1 msg) | **p50 < 20 µs, p99 < 100 µs** | Полный пайплайн, без брокера |
| Аллокации на Publish (JIT) | **≤ 1 KB** | STJ source-gen, pooling |
| Аллокации на Consume (JIT) | **≤ 2 KB** | Scope+deserialize+dispatch |
| Publish → Kafka → Handler (1 KB) | **p50 < 3 ms, p99 < 20 ms** | Локальный Kafka, acks=all |
| Throughput InMemory, 1 producer / 1 consumer | **≥ 1M msg/s** | ThreadRipper, .NET 10 AOT |
| Throughput Rabbit fan-out 1→8, 512B | **≥ 100k msg/s** | На узле 8 cores |
| Cold start воркера (JIT) | **≤ 500 ms** до 1st consumed |  |
| Cold start воркера (AOT) | **≤ 100 ms** до 1st consumed | Ключевое для serverless |
| RSS минимального AOT-воркера | **≤ 30 MB** |  |

## 3. Матрица бенчмарков

```csharp
// AvtoBus.Benchmarks/PublishBench.cs
[MemoryDiagnoser, SimpleJob(RuntimeMoniker.Net100)]
[SimpleJob(RuntimeMoniker.NativeAot90)]
public class PublishBench
{
    private IBus _bus = null!;
    private OrderPlaced _msg = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection().AddAvtoBus(b => b.UseInMemory());
        _bus = services.BuildServiceProvider().GetRequiredService<IBus>();
        _msg = new OrderPlaced(Guid.NewGuid(), 100m, "USD");
    }

    [Benchmark]
    public ValueTask Publish_InMemory() => _bus.Publish(_msg);
}
```

Категории:
1. **micro**: сериализация, диспетчеризация, envelope build
2. **pipeline**: полный проход middleware (5 стандартных)
3. **transport**: send throughput, receive throughput, RTT для каждого транспорта
4. **outbox**: enqueue rate, relay throughput, конкурентные реплики
5. **saga**: throughput простой саги (2 сообщения) и durable-execution (5 шагов)
6. **es**: append rate, load aggregate p99, replay 1M events

## 4. Сравнение с конкурентами (методология)

```
Одинаковое железо (GitHub Actions runner spec / фиксированный EC2 c6a.4xlarge).
Одинаковые payload и топология.
Публикация:  bus.Publish(new PriceTick(...))  size ≈ 200B
Обработка:   пустой хендлер + счётчик
Транспорт:   RabbitMQ 4.x quorum queue, prefetch=64
Метрика:     msg/s, p99 latency, аллокации/msg, RSS
```

Таблица результатов заполняется только артефактами CI. До первого измерения значения остаются `TBD`:

| Framework | msg/s (публикация) | p99 latency | Alloc/msg |
|-----------|-------------------:|------------:|----------:|
| **AvtoBus** (JIT) | TBD | TBD | TBD |
| **AvtoBus** (AOT) | TBD | TBD | TBD |
| Wolverine | TBD | TBD | TBD |
| MassTransit | TBD | TBD | TBD |
| Rebus | TBD | TBD | TBD |
| CAP | TBD | TBD | TBD |

Результат принимается только вместе с commit SHA, конфигурацией среды, исходным JSON BenchmarkDotNet и описанием topology/payload.

## 5. Chaos-тесты (Jepsen-lite)

`avtobus chaos run --profile poweroff` в цикле:
1. Публикация 100k помеченных сообщений с монотонным seq.
2. Каждые 5 сек — kill -9 случайного воркера / rabbit-node.
3. Ожидание тишины.
4. Проверка: `count(consumed) == count(sent)`, дубли ≤ настроенного окна, monotonicity within partition.

Профили: `poweroff`, `network-partition`, `slow-disk`, `broker-restart`, `db-failover`.

## 6. Perf-регрессия как gate PR

```yaml
# .github/workflows/perf.yml
- run: dotnet run -c Release --project AvtoBus.Benchmarks -- --filter '*Publish*'
- run: pwsh ./scripts/compare-bench.ps1 --baseline main --tolerance 5%
```

Регрессия > 5% throughput или > 10% аллокаций → PR блокируется.

## 7. Capacity planner (idea 385)

Формула M/M/c с поправками:

```
λ = rate (msg/s)
μ = 1 / mean_service_time (s)
c = число воркеров (или партиций)
ρ = λ / (c·μ)

При ρ < 0.7  → безопасно, лаг ≈ 0
При 0.7 ≤ ρ < 0.85 → приемлемо, average queue ~1..3
При ρ ≥ 0.85 → рост очереди экспоненциальный, нужен +c
```

CLI:
```
$ avtobus capacity plan --rate 5000 --p99-latency 200ms --handler-time 40ms
Recommendation:
  workers per replica: 6 (max_parallelism)
  replicas: 3
  partitions: 24  (headroom 25%)
  expected p99 in-queue delay: 85ms
  ρ = 0.71 (healthy)
```

## 8. Что мы НЕ обещаем и почему

- «Миллионы msg/s single-node на любых сценариях» — маркетинг. Всегда указываем payload/топологию.
- «Zero-latency» — не бывает. Указываем p50/p95/p99, а не «average».
- «Beats X 10×» — сравниваем на **их** конфигах тоже, не подгоняем.
