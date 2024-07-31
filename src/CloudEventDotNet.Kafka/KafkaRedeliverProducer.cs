
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// 将错误消息重新发布的Kafka生产者
/// </summary>
internal class KafkaRedeliverProducer
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ILogger _logger;

    public KafkaRedeliverProducer(KafkaSubscribeOptions options, ILoggerFactory loggerFactory)
    {
        //创建一个生产者配置对象，设置引导服务器地址、确认机制和消息发送延迟
        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };
        _logger = loggerFactory.CreateLogger<KafkaRedeliverProducer>();
        //使用ProducerBuilder创建一个生产者实例，并设置错误处理和日志处理回调函数
        _producer = new ProducerBuilder<byte[], byte[]>(producerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Producer error: {e}", e))
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log((LogLevel)level, "Producer log: {message}", log);
            })
            .Build();
    }

    /// <summary>
    /// 生产者实例将消息重新发送到相同的主题
    /// </summary>
    /// <param name="consumeResult"></param>
    /// <returns></returns>
    public Task ReproduceAsync(ConsumeResult<byte[], byte[]> consumeResult)
    {
        return _producer.ProduceAsync(consumeResult.Topic, consumeResult.Message);
    }
}
