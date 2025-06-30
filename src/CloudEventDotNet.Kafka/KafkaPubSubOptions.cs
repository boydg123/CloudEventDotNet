using Confluent.Kafka;

namespace CloudEventDotNet;

/// <summary>
/// Kafka 发布和订阅的配置选项。
/// </summary>
public class KafkaPublishOptions
{
    /// <summary>
    /// Confluent.Kafka 生产者配置。
    /// </summary>
    public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig();
}

/// <summary>
/// Kafka 订阅配置选项。
/// </summary>
public class KafkaSubscribeOptions
{
    /// <summary>
    /// Confluent.Kafka 消费者配置。
    /// </summary>
    public ConsumerConfig ConsumerConfig { get; set; } = new ConsumerConfig();

    /// <summary>
    /// 消息投递保证级别。默认为 AtMostOnce。
    /// </summary>
    public DeliveryGuarantee DeliveryGuarantee { get; set; } = DeliveryGuarantee.AtMostOnce;

    /// <summary>
    /// 每个分区正在处理的最大工作项数量限制。
    /// 仅在 AtLeastOnce 模式下生效。
    /// 0表示无限制。默认为 1024。
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 1024;
}

/// <summary>
/// 消息投递保证级别枚举。
/// </summary>
public enum DeliveryGuarantee
{
    /// <summary>
    /// 最多一次。消息拉取后立即提交偏移量，性能高，但可能丢失消息。
    /// </summary>
    AtMostOnce,
    /// <summary>
    /// 至少一次。消息处理成功后才提交偏移量，保证不丢消息，但可能重复处理。
    /// </summary>
    AtLeastOnce
}
