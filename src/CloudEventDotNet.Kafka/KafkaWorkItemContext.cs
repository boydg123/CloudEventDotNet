using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// ������������
/// </summary>
/// <param name="Registry"></param>
/// <param name="Producer"></param>
internal record KafkaWorkItemContext(
    Registry Registry,
    KafkaRedeliverProducer Producer);
