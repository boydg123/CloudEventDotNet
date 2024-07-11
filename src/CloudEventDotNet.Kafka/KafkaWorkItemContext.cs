using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// 工作项上下文
/// </summary>
/// <param name="Registry"></param>
/// <param name="Producer"></param>
internal record KafkaWorkItemContext(
    Registry Registry,
    KafkaRedeliverProducer Producer);
