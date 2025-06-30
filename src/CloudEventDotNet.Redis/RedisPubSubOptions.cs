using System.ComponentModel.DataAnnotations;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis 发布/订阅通用配置选项基类。
/// 封装 Redis 连接工厂和数据库编号。
/// </summary>
public abstract class RedisPubSubOptions
{
    /// <summary>
    /// Redis 连接工厂方法。
    /// 用于获取 IConnectionMultiplexer 实例。
    /// </summary>
    [Required]
    public Func<IConnectionMultiplexer> ConnectionMultiplexerFactory { get; set; } = default!;

    /// <summary>
    /// Redis 数据库编号。
    /// </summary>
    public int Database { get; set; }
}

/// <summary>
/// Redis 发布配置选项。
/// 包含 Stream 最大长度等参数。
/// </summary>
public class RedisPublishOptions : RedisPubSubOptions
{
    /// <summary>
    /// Stream 的最大长度。达到该长度时，Redis 会自动裁剪以控制内存占用。
    /// 默认为 null（不限制）。
    /// </summary>
    public int? MaxLength { get; set; }
}

/// <summary>
/// Redis 订阅配置选项。
/// 包含消费者组、批量拉取、并发等参数。
/// </summary>
public class RedisSubscribeOptions : RedisPubSubOptions
{
    /// <summary>
    /// 消费者组名称。
    /// </summary>
    [Required]
    public string ConsumerGroup { get; set; } = default!;

    /// <summary>
    /// 每批次拉取的 CloudEvent 数量。
    /// </summary>
    public int PollBatchSize { get; set; } = 100;

    /// <summary>
    /// 拉取 CloudEvent 的轮询间隔，默认 15 秒。
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// 允许同时处理的最大 CloudEvent 数量。
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 128;

    /// <summary>
    /// CloudEvent 在被重新投递前需等待的最小挂起时间，默认 60 秒。
    /// </summary>
    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromSeconds(60);
}
