# ADR-0006: Benchmark SLO — методология замера и целевые значения

- Статус: Proposed
- Дата: 2026
- Область: производительность, release gate 5

## Контекст

`docs/20-benchmarks.md §2` задаёт целевые SLO ядра (например, Publish → InMemory → Handler
p50 < 20 µs, p99 < 100 µs; аллокации ≤ 1/2 KB). Release gate 5 (`34-verification-matrix.md`)
требует: «Benchmark SLO либо подтверждены, либо скорректированы ADR-ом без скрытого
изменения результатов». Пока нет ни benchmark-проекта, ни зафиксированной методологии —
все числа являются целями-гипотезами.

## Решение

1. Планируется воспроизводимый проект `benchmarks/AvtoBus.Benchmarks` (BenchmarkDotNet,
   .NET 10, Release, RyuJIT). Сырые артефакты — `BenchmarkDotNet.Artifacts/results/*.json|csv|md`.
2. Методика фиксируется навсегда (редактируется только ADR-ом):
   - payload `OrderPlaced(Guid, decimal, string)` ≈ 200 B;
   - топология: in-memory транспорт, один консьюмер, пустой handler, `bounded channel capacity=1`
     как round-trip семафор;
   - метрики: `PublishOnly_InMemory` (только путь публикации) и `Publish_InMemory`
     (полный round-trip до завершения хендлера);
   - `MemoryDiagnoser` фиксирует аллокации только потока бенчмарка; аллокации consumer thread
     не входят в колонку `Allocated` BenchmarkDotNet и не заявляются без отдельного замера.
3. Целевые значения (гипотезы до первого измерения, из `20-benchmarks.md §2`):

   | Бенчмарк | Target |
   |---|---:|
   | `PublishOnly_InMemory` | p50 < 20 µs |
   | `Publish_InMemory` (round-trip) | p99 < 100 µs |
   | Аллокации | ≤ 1/2 KB |

   Измеренные значения появятся только после реализации `benchmarks/AvtoBus.Benchmarks`
   и первого CI-артефакта job `benchmarks`.
4. До получения CI-артефакта строки матрицы «Allocation/Latency SLO» остаются `TBD`,
   а значения из `20-benchmarks.md §2` считаются целями.
5. Критично: никакое число в маркетинговых материалах не публикуется без commit SHA,
   конфигурации среды, исходного JSON BenchmarkDotNet и описания topology/payload
   (`20-benchmarks.md §4`).

## Последствия

Положительные:

- Появляется воспроизводимая база для регрессионного сравнения (perf gate на PR, идея 310).
- Release gate 5 закрывается честно: измеренные значения и методология фиксируются,
  а не подгоняются под цели.

Отрицательные:

- До появления benchmark-проекта значения остаются целями; подтверждение возможно только
  на референсном железе после реализации.
- Consumer-аллокации не заявляются — нужен отдельный замер внутри хендлера.

## Проверка решения

- `dotnet run -c Release --project benchmarks/AvtoBus.Benchmarks --filter *PublishLatencyBench*`
  должен воспроизводить baseline на том же железе (в пределах StdDev) — ожидается после
  реализации проекта.
- CI job `benchmarks` публикует сырые артефакты (release gate 6).
- Строки матрицы «Allocation SLO»/«Latency SLO» ссылаются на этот ADR и `20-benchmarks.md §2.1`.

## Отклонённые варианты

1. Просто объявить SLO «выполненными» без замеров — скрытое изменение результатов, запрещено gate 5.
2. Гнать бенчмарки на референсном железе в момент ADR — невозможно локально; вынесено в CI.
