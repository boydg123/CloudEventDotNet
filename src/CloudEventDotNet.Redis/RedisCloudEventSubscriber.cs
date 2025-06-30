using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis CloudEvent 订阅者实现。
/// 实现 ICloudEventSubscriber，负责启动和停止所有 Redis 消息通道。
/// </summary>
internal class RedisCloudEventSubscriber : ICloudEventSubscriber
{
    /// <summary>
    /// PubSub 名称。
    /// </summary>
    private readonly string _pubSubName;
    /// <summary>
    /// 日志记录器。
    /// </summary>
    private readonly ILogger _logger;
    /// <summary>
    /// Redis 消息通道工厂。
    /// </summary>
    private readonly RedisMessageChannelFactory _channelFactory;
    /// <summary>
    /// 当前所有订阅的消息通道。
    /// </summary>
    private RedisMessageChannel[]? _subscribers;

    /// <summary>
    /// 构造函数，注入依赖。
    /// </summary>
    public RedisCloudEventSubscriber(
        string pubSubName,
        ILogger<RedisCloudEventSubscriber> logger,
        RedisMessageChannelFactory channelFactory)
    {
        _pubSubName = pubSubName;
        _logger = logger;
        _channelFactory = channelFactory;
    }

    /// <summary>
    /// 启动所有消息通道。
    /// </summary>
    public Task StartAsync()
    {
        _logger.LogDebug("Subscribe starting");
        _subscribers = _channelFactory.Create(_pubSubName);
        return Task.CompletedTask;
    }

    /// <summary>
    /// 停止所有消息通道。
    /// </summary>
    public async Task StopAsync()
    {
        await Task.WhenAll(_subscribers!.Select(s => s.StopAsync()));
    }
}
