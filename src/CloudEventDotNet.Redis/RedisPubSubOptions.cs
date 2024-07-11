using System.ComponentModel.DataAnnotations;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis PubSub 选项
/// </summary>
public abstract class RedisPubSubOptions
{
    /// <summary>
    /// 一个用于解析 Redis 发布订阅所使用的 IConnectionMultiplexer 的工厂。
    /// </summary>
    [Required]
    public Func<IConnectionMultiplexer> ConnectionMultiplexerFactory { get; set; } = default!;

    /// <summary>
    /// 数据库
    /// </summary>
    public int Database { get; set; }
}

/// <summary>
/// Redis 发布选项
/// </summary>
public class RedisPublishOptions : RedisPubSubOptions
{
    /// <summary>
    /// 最大流中的项目数。 当指定的长度达到时，旧的条目将自动淘汰，以保持流的大小始终保持不变。 默认为无限。
    /// </summary>
    public int? MaxLength { get; set; }
}

/// <summary>
/// Redis 订阅选项
/// </summary>
public class RedisSubscribeOptions : RedisPubSubOptions
{
    /// <summary>
    /// 订阅者的消费者组
    /// </summary>
    [Required]
    public string ConsumerGroup { get; set; } = default!;

    /// <summary>
    /// 每次拉去的最大 CloudEvent 数量
    /// </summary>
    public int PollBatchSize { get; set; } = 100;

    /// <summary>
    /// 轮询新CloudEvent的间隔时间，默认15秒
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// 本地进程队列中未处理的CloudEvents的限制。
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 128;

    /// <summary>
    /// The amount time a CloudEvent must be pending before attempting to redeliver it. Defaults to "60s".
    /// CloudEvent在尝试重新传递之前必须挂起的时间。默认值为“60s”
    /// </summary>
    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromSeconds(60);
}
