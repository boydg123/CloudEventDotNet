using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis消息通道工厂
/// </summary>
internal class RedisMessageChannelFactory
{
    // 主要功能：RedisMessageChannelFactory 类的主要功能是创建和管理 Redis 消息通道。它通过依赖注入获取必要的配置和工具，然后根据给定的 pubSubName 创建相应的 Redis
    // 消息通道数组。每个通道都包含详细的上下文信息和遥测记录，以便更好地管理和监控 Redis 消息的订阅和处理.
    // 关键步骤：
    // 1. 通过依赖注入获取 IOptionsFactory<RedisSubscribeOptions>、Registry、IServiceScopeFactory 和 ILoggerFactory 实例。
    // 2. 根据 pubSubName 创建 RedisSubscribeOptions 实例，并获取 ConnectionMultiplexer 和 Redis 数据库实例。
    // 3. 获取订阅的主题列表，并为每个主题创建 RedisMessageChannel 实例。
    // 4. 为每个通道创建详细的上下文信息和遥测记录，最终返回这些通道的数组。

    private readonly IOptionsFactory<RedisSubscribeOptions> _optionsFactory;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILoggerFactory _loggerFactory;

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
    /// 创建 Redis 消息通道数组
    /// </summary>
    /// <param name="pubSubName"></param>
    /// <returns></returns>
    public RedisMessageChannel[] Create(string pubSubName)
    {
        var options = _optionsFactory.Create(pubSubName);
        var multiplexer = options.ConnectionMultiplexerFactory();
        var redis = multiplexer.GetDatabase(options.Database);

        // 获取订阅的主题列表，并为每个主题调用 Create 方法创建 RedisMessageChannel 实例，最终返回这些实例的数组
        return _registry.GetSubscribedTopics(pubSubName)
            .Select(topic => Create(pubSubName, topic, options, redis)).ToArray();
    }

    /// <summary>
    /// 创建单个 Redis 消息通道
    /// </summary>
    /// <param name="pubSubName"></param>
    /// <param name="topic"></param>
    /// <param name="options"></param>
    /// <param name="redis"></param>
    /// <returns></returns>
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
