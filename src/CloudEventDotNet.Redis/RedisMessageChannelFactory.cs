using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis 消息通道工厂。
/// 负责为每个 PubSub/Topic 创建和管理 RedisMessageChannel 实例，实现多 Topic 的独立消费和资源隔离。
/// </summary>
internal class RedisMessageChannelFactory
{
    /// <summary>
    /// Redis 订阅配置工厂。
    /// </summary>
    private readonly IOptionsFactory<RedisSubscribeOptions> _optionsFactory;
    /// <summary>
    /// 事件注册表。
    /// </summary>
    private readonly Registry _registry;
    /// <summary>
    /// 依赖注入作用域工厂。
    /// </summary>
    private readonly IServiceScopeFactory _scopeFactory;
    /// <summary>
    /// 日志工厂。
    /// </summary>
    private readonly ILoggerFactory _loggerFactory;

    /// <summary>
    /// 构造函数，注入依赖。
    /// </summary>
    public RedisMessageChannelFactory(
        IOptionsFactory<RedisSubscribeOptions> optionsFactory,
        Registry registry,
        IServiceScopeFactory scopeFactory,
        ILoggerFactory loggerFactory)
    {
        _optionsFactory = optionsFactory;
        _registry = registry;
        _scopeFactory = scopeFactory;
        _loggerFactory = loggerFactory;
    }

    /// <summary>
    /// 创建指定 PubSub 下所有 Topic 的 Redis 消息通道。
    /// </summary>
    /// <param name="pubSubName">PubSub 名称</param>
    /// <returns>所有 Topic 的 RedisMessageChannel 实例数组</returns>
    public RedisMessageChannel[] Create(string pubSubName)
    {
        var options = _optionsFactory.Create(pubSubName);
        var multiplexer = options.ConnectionMultiplexerFactory();
        var redis = multiplexer.GetDatabase(options.Database);

        // 获取所有已订阅的 Topic，为每个 Topic 创建 RedisMessageChannel
        return _registry.GetSubscribedTopics(pubSubName)
            .Select(topic => Create(pubSubName, topic, options, redis)).ToArray();
    }

    /// <summary>
    /// 创建单个 Topic 的 Redis 消息通道。
    /// </summary>
    /// <param name="pubSubName">PubSub 名称</param>
    /// <param name="topic">消息主题</param>
    /// <param name="options">订阅配置</param>
    /// <param name="redis">Redis 数据库实例</param>
    /// <returns>RedisMessageChannel 实例</returns>
    private RedisMessageChannel Create(
        string pubSubName,
        string topic,
        RedisSubscribeOptions options,
        IDatabase redis)
    {
        var logger = _loggerFactory.CreateLogger($"{nameof(RedisCloudEventSubscriber)}[{pubSubName}:{topic}]");
        var channelContext = new RedisMessageChannelContext(
            pubSubName,
            redis.Multiplexer.ClientName,
            options.ConsumerGroup,
            topic
        );
        var workItemContext = new RedisWorkItemContext(
            _registry,
            _scopeFactory,
            redis,
            _loggerFactory
        );
        var channel = new RedisMessageChannel(
            options,
            redis,
            channelContext,
            workItemContext,
            _loggerFactory);
        return channel;
    }

}
