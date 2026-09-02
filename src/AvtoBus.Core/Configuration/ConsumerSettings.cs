namespace AvtoBus.Configuration;

/// <summary>Настройки одного консьюмера: параллелизм, батчинг, упорядочивание.</summary>
public sealed class ConsumerSettings
{
    public required Type MessageType { get; init; }

    /// <summary>Сколько сообщений обрабатывается одновременно.</summary>
    public int MaxParallelism { get; set; } = 1;

    /// <summary>Сколько сообщений транспорт отдаёт до подтверждения.</summary>
    public int PrefetchCount { get; set; } = 32;

    /// <summary>Размер батча; 1 — батчинг выключен (идея 20).</summary>
    public int BatchSize { get; set; } = 1;

    /// <summary>Максимальное ожидание добора батча.</summary>
    public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Число партиций для упорядоченной обработки по ключу; 0 — без партиционирования (идея 25).</summary>
    public int Partitions { get; set; }

    /// <summary>Извлекает ключ упорядочивания из сообщения.</summary>
    public Func<object, string>? PartitionKeySelector { get; set; }

    /// <summary>Окно debounce по ключу; сливает шквал апдейтов в один (идея 30).</summary>
    public TimeSpan? DebounceWindow { get; set; }

    public Func<object, string>? DebounceKeySelector { get; set; }

    /// <summary>Явное имя очереди вместо вычисленного по конвенции.</summary>
    public string? QueueName { get; set; }

    /// <summary>Группа консьюмеров: определяет, делят ли реплики нагрузку или дублируют её.</summary>
    public string? ConsumerGroup { get; set; }
}

/// <summary>Fluent-настройка консьюмера.</summary>
public sealed class ConsumerConfigurator<T>(ConsumerSettings settings) where T : class
{
    public ConsumerSettings Settings { get; } = settings;

    /// <summary>N сообщений обрабатываются одновременно (идея 358).</summary>
    public ConsumerConfigurator<T> MaxParallelism(int value)
    {
        if (value < 1)
            throw new ArgumentOutOfRangeException(nameof(value), value, "Параллелизм должен быть не меньше 1.");

        Settings.MaxParallelism = value;
        return this;
    }

    public ConsumerConfigurator<T> Prefetch(int count)
    {
        if (count < 1) throw new ArgumentOutOfRangeException(nameof(count), "Prefetch must be >=1");
        Settings.PrefetchCount = count;
        return this;
    }

    /// <summary>Батчевая обработка: один хендлер на группу сообщений (идея 20).</summary>
    public ConsumerConfigurator<T> Batch(int size, TimeSpan? timeout = null, Func<T, string>? partitionBy = null)
    {
        if (size < 1) throw new ArgumentOutOfRangeException(nameof(size));
        if (timeout is { } t && t <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        Settings.BatchSize = size;
        if (timeout is { } value)
            Settings.BatchTimeout = value;
        if (partitionBy is not null)
            Settings.PartitionKeySelector = message => partitionBy((T)message);
        return this;
    }

    /// <summary>
    /// Строгий порядок в рамках ключа при параллельной обработке разных ключей (идея 25).
    /// </summary>
    public ConsumerConfigurator<T> OrderedBy(Func<T, string> keySelector, int partitions = 8)
    {
        if (partitions < 1) throw new ArgumentOutOfRangeException(nameof(partitions));
        Settings.PartitionKeySelector = message => keySelector((T)message);
        Settings.Partitions = partitions;
        return this;
    }

    /// <summary>Сливает поток обновлений одного ключа в одно сообщение (идея 30).</summary>
    public ConsumerConfigurator<T> Debounce(Func<T, string> keySelector, TimeSpan window)
    {
        if (window <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(window));
        Settings.DebounceKeySelector = message => keySelector((T)message);
        Settings.DebounceWindow = window;
        return this;
    }

    public ConsumerConfigurator<T> FromQueue(string queueName)
    {
        Settings.QueueName = queueName;
        return this;
    }

    public ConsumerConfigurator<T> InGroup(string consumerGroup)
    {
        Settings.ConsumerGroup = consumerGroup;
        return this;
    }
}
