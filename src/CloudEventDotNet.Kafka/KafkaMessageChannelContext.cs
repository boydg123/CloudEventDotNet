using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 消息通道上下文。
/// 包含了与特定消息通道（即特定分区）相关的上下文信息。
/// </summary>
/// <param name="PubSubName">PubSub 名称</param>
/// <param name="ConsumerName">消费者名称（client.id）</param>
/// <param name="ConsumerGroup">消费者组</param>
/// <param name="TopicPartition">主题分区</param>
public record KafkaMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    TopicPartition TopicPartition
);
