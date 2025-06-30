using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 工作项上下文。
/// 包含了处理 Kafka 消息工作项所需的全局依赖，如注册中心和重发生产者。
/// </summary>
internal readonly struct KafkaWorkItemContext
{
    /// <summary>
    /// 事件注册中心。
    /// </summary>
    public Registry Registry { get; }
    /// <summary>
    /// 失败消息重发生产者。
    /// </summary>
    public KafkaRedeliverProducer Producer { get; }

    public KafkaWorkItemContext(Registry registry, KafkaRedeliverProducer producer)
    {
        Registry = registry;
        Producer = producer;
    }
}
