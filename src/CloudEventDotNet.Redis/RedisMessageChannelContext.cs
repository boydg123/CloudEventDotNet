namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis 消息通道上下文。
/// 封装了与特定 PubSub、Topic、消费者组、消费者相关的上下文信息，
/// 用于 Redis 消息通道的唯一标识和上下文传递。
/// </summary>
/// <param name="PubSubName">PubSub 名称，标识一组发布/订阅配置</param>
/// <param name="ConsumerName">消费者名称，标识具体的消费实例</param>
/// <param name="ConsumerGroup">消费者组名称，Redis Stream 的 Group</param>
/// <param name="Topic">消息主题（Stream key）</param>
internal sealed record RedisMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    string Topic
);
