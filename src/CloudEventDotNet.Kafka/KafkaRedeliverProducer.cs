
using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// ��������Ϣ���·�����Kafka������
/// </summary>
internal class KafkaRedeliverProducer
{
    private readonly IProducer<byte[], byte[]> _producer;

    public KafkaRedeliverProducer(KafkaSubscribeOptions options, KafkaConsumerTelemetry telemetry)
    {
        //����һ�����������ö�������������������ַ��ȷ�ϻ��ƺ���Ϣ�����ӳ�
        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };

        //ʹ��ProducerBuilder����һ��������ʵ���������ô��������־����ص�����
        _producer = new ProducerBuilder<byte[], byte[]>(producerConfig)
            .SetErrorHandler((_, e) => telemetry.OnProducerError(e))
            .SetLogHandler((_, log) => telemetry.OnProducerLog(log))
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
