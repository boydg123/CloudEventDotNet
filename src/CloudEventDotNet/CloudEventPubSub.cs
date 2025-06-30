using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 发布/订阅核心实现。
/// 负责将事件数据封装为 CloudEvent 并发布到对应的消息中间件。
/// </summary>
internal sealed class CloudEventPubSub : ICloudEventPubSub
{
    // PubSub 配置选项
    private readonly PubSubOptions _options;
    // 发布者工厂字典，key为PubSub名称，value为具体发布者实例
    private readonly Dictionary<string, ICloudEventPublisher> _publishers;
    // 日志工厂
    private readonly ILoggerFactory _loggerFactory;
    // 日志实例
    private readonly ILogger _logger;
    // 注册中心，负责事件元数据和处理器的查找
    private readonly Registry _registry;

    /// <summary>
    /// 构造函数，注入依赖项。
    /// </summary>
    /// <param name="services">依赖注入容器</param>
    /// <param name="registry">注册中心</param>
    /// <param name="options">发布/订阅配置</param>
    /// <param name="loggerFactory">日志工厂</param>
    public CloudEventPubSub(
        IServiceProvider services,
        Registry registry,
        IOptions<PubSubOptions> options,
        ILoggerFactory loggerFactory)
    {
        _options = options.Value;
        _loggerFactory = loggerFactory;
        // 通过工厂方法创建所有已注册的发布者实例
        _publishers = _options.PublisherFactoris.ToDictionary(kvp => kvp.Key, kvp => kvp.Value(services));
        _logger = loggerFactory.CreateLogger<CloudEventPubSub>();
        _registry = registry;
    }

    /// <summary>
    /// 发布事件。
    /// 将强类型数据封装为 CloudEvent，并路由到对应的消息中间件。
    /// </summary>
    /// <typeparam name="TData">事件数据类型</typeparam>
    /// <param name="data">事件数据</param>
    /// <returns>异步任务</returns>
    public async Task PublishAsync<TData>(TData data)
    {
        // 获取数据类型
        var dataType = typeof(TData);

        // 查找事件元数据（如 topic、pubsub 名称等）
        var metadata = _registry.GetMetadata(dataType);

        // 构建 CloudEvent 对象
        var cloudEvent = new CloudEvent<TData>(
            Id: Guid.NewGuid().ToString(),
            Source: metadata.Source,
            Type: metadata.Type,
            Time: DateTimeOffset.UtcNow,
            Data: data,
            DataSchema: null,
            Subject: null
        );
        _logger.LogDebug("Publishing cloud event: {cloudEvent}, Metadata: {metadata}", JSON.Serialize(cloudEvent), JSON.Serialize(metadata));

        // 路由到对应的发布者并发布
        var publisher = _publishers[metadata.PubSubName];
        await publisher.PublishAsync(metadata.Topic, cloudEvent).ConfigureAwait(false);

        _logger.LogDebug("Published CloudEvent:{metadata}", JSON.Serialize(metadata));
    }
}
