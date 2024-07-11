namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis消息通道上下文
/// </summary>
/// <param name="PubSubName"></param>
/// <param name="ConsumerName"></param>
/// <param name="ConsumerGroup"></param>
/// <param name="Topic"></param>
internal sealed record RedisMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    string Topic
);
