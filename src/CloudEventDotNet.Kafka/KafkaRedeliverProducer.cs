using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 失败消息重发生产者。
/// 负责将处理失败的消息重新发送到 Kafka 的同一分区，以保证消息的顺序性。
/// </summary>
internal sealed class KafkaRedeliverProducer
{
    // Confluent.Kafka 生产者实例
    private readonly IProducer<byte[], byte[]> _producer;
    // 日志实例
    private readonly ILogger _logger;

    public KafkaRedeliverProducer(KafkaSubscribeOptions options, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<KafkaRedeliverProducer>();

        // 创建一个新的生产者配置用于重发
        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };
        _producer = new ProducerBuilder<byte[], byte[]>(producerConfig)
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log((LogLevel)level, "Producer log: {message}", log);
            })
            .Build();
    }

    /// <summary>
    /// 重新发送处理失败的消息。
    /// </summary>
    /// <param name="consumeResult">原始的消费结果</param>
    /// <returns>异步任务</returns>
    public async Task ReproduceAsync(ConsumeResult<byte[], byte[]> consumeResult)
    {
        _logger.LogInformation("Reproducing message {message}", consumeResult.TopicPartitionOffset);
        await _producer.ProduceAsync(consumeResult.TopicPartition, consumeResult.Message).ConfigureAwait(false);
    }
}
