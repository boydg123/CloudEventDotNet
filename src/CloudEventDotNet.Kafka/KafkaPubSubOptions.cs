using Confluent.Kafka;

namespace CloudEventDotNet;

/// <summary>
/// Kafka发布者配置
/// </summary>
public class KafkaPublishOptions
{
    /// <summary>
    /// Kafka生产者配置.
    /// </summary>
    public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig();
}

/// <summary>
/// Kafka订阅者配置
/// </summary>
public class KafkaSubscribeOptions
{
    /// <summary>
    /// Kafka消费者配置.
    /// </summary>
    public ConsumerConfig ConsumerConfig { get; set; } = new ConsumerConfig();

    /// <summary>
    /// 消息处理保证. 默认是AtMostOnce
    /// </summary>
    public DeliveryGuarantee DeliveryGuarantee { get; set; } = DeliveryGuarantee.AtMostOnce;

    /// <summary>
    /// 本地进程队列中未处理的CloudEvents的限制.默认1024
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 1024;
}

public enum DeliveryGuarantee
{
    AtMostOnce, AtLeastOnce
}
