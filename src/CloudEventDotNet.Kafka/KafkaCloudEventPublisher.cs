using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

/// <summary>
/// CloudEvent Kafka 发布者实现。
/// 负责将 CloudEvent 发布到 Kafka 指定的主题。
/// </summary>
internal sealed class KafkaCloudEventPublisher : ICloudEventPublisher
{
    // Confluent.Kafka 生产者实例
    private readonly IProducer<byte[], byte[]> _producer;
    // 日志实例
    private readonly ILogger _logger;

    /// <summary>
    /// 构造函数，初始化 Kafka 生产者。
    /// </summary>
    /// <param name="pubSubName">PubSub 名称</param>
    /// <param name="options">发布配置</param>
    /// <param name="loggerFactory">日志工厂</param>
    public KafkaCloudEventPublisher(
        string pubSubName,
        KafkaPublishOptions options,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger(nameof(KafkaCloudEventPublisher));
        // 使用 ProducerBuilder 创建生产者，并设置错误和日志处理器
        _producer = new ProducerBuilder<byte[], byte[]>(options.ProducerConfig)
            .SetErrorHandler((_, e) => _logger.LogError($"Producer error: {e}"))
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log((LogLevel)level, "Producer log: {message}", log);
            })
            .Build();
    }

    /// <summary>
    /// 发布 CloudEvent 到 Kafka。
    /// 将 CloudEvent 序列化为 UTF-8 字节后发送。
    /// </summary>
    /// <typeparam name="TData">事件数据类型</typeparam>
    /// <param name="topic">Kafka 主题</param>
    /// <param name="cloudEvent">要发布的 CloudEvent</param>
    /// <returns>异步任务</returns>
    public async Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent)
    {
        var message = new Message<byte[], byte[]>
        {
            Value = JSON.SerializeToUtf8Bytes(cloudEvent)
        };

        // 异步发送消息
        DeliveryResult<byte[], byte[]> result = await _producer.ProduceAsync(topic, message).ConfigureAwait(false);

        _logger.LogDebug($"Produced message {result.Topic}:{result.Partition.Value}:{result.Offset.Value}");
    }
}
