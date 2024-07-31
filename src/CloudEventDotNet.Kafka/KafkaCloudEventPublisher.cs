using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

/// <summary>
/// CloudEvent Kafka 发布者
/// </summary>
internal sealed class KafkaCloudEventPublisher : ICloudEventPublisher
{
    private readonly IProducer<byte[], byte[]> _producer; // Kafka producer
    private readonly ILogger _logger;

    public KafkaCloudEventPublisher(
        string pubSubName,
        KafkaPublishOptions options,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger(nameof(KafkaCloudEventPublisher));
        _producer = new ProducerBuilder<byte[], byte[]>(options.ProducerConfig)
            .SetErrorHandler((_, e) => _logger.LogError($"Producer error: {e}"))
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log((LogLevel)level, "Producer log: {message}", log);
            })
            .Build();
    }

    public async Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent)
    {
        var message = new Message<byte[], byte[]>
        {
            Value = JSON.SerializeToUtf8Bytes(cloudEvent)
        };

        // 生产消息
        DeliveryResult<byte[], byte[]> result = await _producer.ProduceAsync(topic, message).ConfigureAwait(false);

        _logger.LogDebug($"Produced message {result.Topic}:{result.Partition.Value}:{result.Offset.Value}");
    }
}
