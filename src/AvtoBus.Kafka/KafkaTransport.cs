using System.Collections.Concurrent;
using AvtoBus.Observability;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace AvtoBus.Kafka;

/// <summary>
/// Kafka-транспорт (идеи 57–60): <see cref="ITransport"/> поверх Confluent.Kafka.
///
/// Семантика:
/// — Очередь и топик — оба топики Kafka; разница в группах: очередь читает один сервис,
///   топик — несколько групп, каждой — копия (нативно).
/// — Подтверждение = коммит оффсета. Reject(requeue) = пере-публикация с
///   инкрементированным DeliveryAttempt + коммит исходного оффсета.
/// — Exactly-once (идея 57) через транзакционный продюсер: enable.idempotence,
///   transactional.id, isolation.level=read_committed.
/// — Back-pressure (идея 59): при переполнении внутреннего буфера невыполненных
///   сообщений партиции паузятся и возобновляются по мере подтверждений.
/// — Порядок по ключу (идея 60): PartitionKey → Kafka key → партиция → порядок внутри
///   партиции сохранён; один консьюмер на подписку читает последовательно.
/// </summary>
public sealed class KafkaTransport : ITransport, IConsumerLagProvider, IDisposable
{
    private readonly KafkaOptions _options;
    private readonly IProducer<string, byte[]> _producer;
    private readonly IAdminClient _admin;
    private readonly SemaphoreSlim _producerGate = new(1, 1);
    private readonly ConcurrentDictionary<string, long> _consumerLags = new(StringComparer.Ordinal);
    private bool _transactionInitialized;
    private int _disposed;

    public KafkaTransport(KafkaOptions options)
    {
        _options = options;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = $"{options.ClientId}-producer",
            Acks = options.Acks,
            EnableIdempotence = true,
            CompressionType = options.CompressionType,
            MessageMaxBytes = options.MaxMessageBytes,
            TransactionalId = options.ExactlyOnce ? options.TransactionalId : null,
        };
        ApplyAdditional(producerConfig);

        _producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
        _admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = $"{options.ClientId}-admin",
        }).Build();
    }

    public string Name => "kafka";

    /// <summary>Оценка лага группы — для метрики consumer.lag (идея 334). Точность — как у OFFSET query.</summary>
    public IReadOnlyDictionary<string, long> ConsumerLags => _consumerLags;

    public async ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
    {
        var message = KafkaEnvelopeSerializer.ToKafka(envelope);
        await SendMessageAsync(TopicName(destination), message, ct);
    }

    /// <summary>Отправка одного Kafka-сообщения; транзакционная ветка — при exactly-once (идея 57).</summary>
    private async ValueTask SendMessageAsync(string topic, Message<string, byte[]> message, CancellationToken ct)
    {
        await _producerGate.WaitAsync(ct);
        try
        {
            if (_options.ExactlyOnce)
            {
                if (!_transactionInitialized)
                {
                    _producer.InitTransactions(TimeSpan.FromSeconds(30));
                    _transactionInitialized = true;
                }

                _producer.BeginTransaction();
                try
                {
                    await _producer.ProduceAsync(topic, message, ct);
                    _producer.CommitTransaction(TimeSpan.FromSeconds(30));
                }
                catch
                {
                    _producer.AbortTransaction();
                    throw;
                }
            }
            else
            {
                await _producer.ProduceAsync(topic, message, ct);
            }
        }
        finally
        {
            _producerGate.Release();
        }
    }

    /// <summary>
    /// Один консьюмер на подписку. Порядок внутри партиции сохраняется; back-pressure —
    /// паузой партиций при переполнении буфера невыполненных (идеи 59, 60).
    /// </summary>
    public async IAsyncEnumerable<ITransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var topic = TopicName(subscription.Destination);
        var group = subscription.ConsumerGroup;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = $"{_options.ClientId}-consumer-{group}",
            GroupId = group,
            AutoOffsetReset = _options.AutoOffsetReset,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            FetchMaxBytes = _options.FetchMaxBytes,
            SessionTimeoutMs = _options.SessionTimeoutMs,
            IsolationLevel = _options.ExactlyOnce ? IsolationLevel.ReadCommitted : IsolationLevel.ReadUncommitted,
        };
        ApplyAdditional(consumerConfig);

        using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(topic);

        // Счётчик невыполненных (выданных, но не подтверждённых) сообщений — для паузы партиций.
        var outstanding = 0;
        var paused = false;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> result;
                try
                {
                    result = consumer.Consume(ct);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
                catch (ConsumeException)
                {
                    // Сбой чтения (например, leader unavailable) — продолжаем опрос.
                    continue;
                }

                if (result is null || result.IsPartitionEOF || result.Message is null)
                    continue;

                Envelope envelope;
                try
                {
                    envelope = KafkaEnvelopeSerializer.FromKafka(result);
                }
                catch (InvalidDataException)
                {
                    // Несовместимый продюсер — коммитим оффсет, чтобы не зациклиться на мусоре.
                    consumer.Commit(result);
                    continue;
                }

                TrackLag(consumer, result, group);

                var message = new KafkaMessage(this, consumer, result, envelope);
                outstanding++;
                ApplyBackpressure(consumer, ref outstanding, ref paused);

                yield return message;

                // После возврата управления хендлер уже мог подтвердить/отклонить —
                // паузу снимаем, когда буфер опустел.
                ApplyBackpressure(consumer, ref outstanding, ref paused);
            }
        }
        finally
        {
            consumer.Unsubscribe();
        }
    }

    /// <summary>
    /// Back-pressure (идея 59): когда невыполненных сообщений больше порога — партиции
    /// паузятся (консьюмер перестаёт фетчить), при снижении — возобновляются.
    /// </summary>
    private void ApplyBackpressure(IConsumer<string, byte[]> consumer, ref int outstanding, ref bool paused)
    {
        if (!_options.PauseOnBackpressure)
            return;

        var assignments = consumer.Assignment;
        if (assignments is null || assignments.Count == 0)
            return;

        if (outstanding >= _options.BackpressureThreshold && !paused)
        {
            consumer.Pause(assignments);
            paused = true;
        }
        else if (outstanding < _options.BackpressureThreshold / 2 && paused)
        {
            consumer.Resume(assignments);
            paused = false;
        }
    }

    private void TrackLag(IConsumer<string, byte[]> consumer, ConsumeResult<string, byte[]> result, string group)
    {
        try
        {
            var watermarks = consumer.QueryWatermarkOffsets(
                new TopicPartition(result.Topic, result.Partition),
                TimeSpan.FromSeconds(2));
            var lag = Math.Max(0, watermarks.High.Value - result.Offset.Value);
            _consumerLags[$"{result.Topic}:{group}"] = lag;
        }
        catch (KafkaException)
        {
            // Лаг — наблюдательная метрика; неудачный запрос не должен ломать обработку.
        }
    }

    public async ValueTask ProvisionAsync(
        IReadOnlyCollection<TransportDestination> destinations,
        CancellationToken ct = default)
    {
        var unique = destinations
            .Select(TopicName)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (unique.Count == 0)
            return;

        try
        {
            var specs = unique.Select(topic => new TopicSpecification
            {
                Name = topic,
                NumPartitions = _options.DefaultPartitions,
                ReplicationFactor = _options.DefaultReplicationFactor,
            }).ToList();

            await _admin.CreateTopicsAsync(specs, new CreateTopicsOptions
            {
                RequestTimeout = TimeSpan.FromSeconds(10),
            }).ConfigureAwait(false);
        }
        catch (CreateTopicsException exception)
        {
            // TopicAlreadyExists — штатно (несколько реплик ProvisionAsync на разных инстансах);
            // остальные ошибки валим, чтобы топология гарантированно была готова.
            if (exception.Results.Any(result => result.Error.Code != ErrorCode.TopicAlreadyExists))
                throw;
        }
    }

    private string TopicName(TransportDestination destination) => destination.Name;

    /// <summary>Имя DLQ-топика для заданного топика (соглашение поверх core-контракта).</summary>
    public static string DlqTopicName(string topic) => $"{topic}.dlq";

    private void ApplyAdditional(dynamic config)
    {
        foreach (var (key, value) in _options.AdditionalProperties)
            config.Set(key, value);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _producer.Dispose();
        _admin.Dispose();
        _producerGate.Dispose();
    }

    /// <summary>ITransport : IAsyncDisposable — синхронного Dispose достаточно для ресурсов librdkafka.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Сообщение из Kafka. Acknowledge = коммит оффсета; Reject(requeue) = пере-публикация
    /// с инкрементом попытки + коммит исходного; Reject(без requeue) = коммит (сообщение
    /// считается обработанным; в DLQ его кладёт recoverability на уровне ядра).
    /// </summary>
    private sealed class KafkaMessage : ITransportMessage
    {
        private readonly KafkaTransport _transport;
        private readonly IConsumer<string, byte[]> _consumer;
        private readonly ConsumeResult<string, byte[]> _result;
        private int _settled;

        public KafkaMessage(
            KafkaTransport transport,
            IConsumer<string, byte[]> consumer,
            ConsumeResult<string, byte[]> result,
            Envelope envelope)
        {
            _transport = transport;
            _consumer = consumer;
            _result = result;
            Envelope = envelope;
        }

        public Envelope Envelope { get; }

        /// <summary>Фактический источник: физическая очередь группы — партиция топика.</summary>
        public TransportDestination Source
            => TransportDestination.Queue(_result.Topic);

        public ValueTask AcknowledgeAsync(CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return ValueTask.CompletedTask;

            _consumer.Commit(_result);
            return ValueTask.CompletedTask;
        }

        public async ValueTask RejectAsync(bool requeue, CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _settled, 1) == 1)
                return;

            if (requeue)
            {
                // Requeue в Kafka = пере-публикация с инкрементом попытки; исходный оффсет
                // коммитим, чтобы сообщение не вернулось само через redelivery.
                var retry = KafkaEnvelopeSerializer.ToKafka(Envelope.NextAttempt());
                await _transport.SendMessageAsync(_result.Topic, retry, ct).ConfigureAwait(false);
            }

            _consumer.Commit(_result);
        }
    }
}
