
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// ��������Ϣ���·�����Kafka������
/// </summary>
internal class KafkaRedeliverProducer
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ILogger _logger;

    public KafkaRedeliverProducer(KafkaSubscribeOptions options, ILoggerFactory loggerFactory)
    {
        //����һ�����������ö�������������������ַ��ȷ�ϻ��ƺ���Ϣ�����ӳ�
        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };
        _logger = loggerFactory.CreateLogger<KafkaRedeliverProducer>();
        //ʹ��ProducerBuilder����һ��������ʵ���������ô��������־����ص�����
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
    /// ������ʵ������Ϣ���·��͵���ͬ������
    /// </summary>
    /// <param name="consumeResult"></param>
    /// <returns></returns>
    public Task ReproduceAsync(ConsumeResult<byte[], byte[]> consumeResult)
    {
        return _producer.ProduceAsync(consumeResult.Topic, consumeResult.Message);
    }
}
