using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 处理器委托。
/// 用于将 CloudEvent 分发给对应的处理器。
/// </summary>
/// <param name="serviceProvider">依赖注入容器</param>
/// <param name="event">原始 CloudEvent 对象</param>
/// <param name="token">取消令牌</param>
/// <returns>异步任务</returns>
internal delegate Task HandleCloudEventDelegate(IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token);

/// <summary>
/// 注册中心：维护事件类型、元数据和处理器之间的映射关系。
/// 负责事件元数据注册、处理器注册与查找，是 CloudEventDotNet 的核心调度组件。
/// </summary>
public sealed class Registry
{
    // 事件类型到元数据的映射
    private readonly Dictionary<Type, CloudEventMetadata> _metadata = new();
    // 元数据到处理器委托的映射
    private readonly Dictionary<CloudEventMetadata, HandleCloudEventDelegate> _handlerDelegates = new();
    // 元数据到处理器实例的映射
    private readonly Dictionary<CloudEventMetadata, CloudEventHandler> _handlers = new();
    // 默认 PubSub 名称、Topic、Source
    private readonly string _defaultPubSubName;
    private readonly string _defaultTopic;
    private readonly string _defaultSource;

    /// <summary>
    /// 构造函数，初始化默认 PubSub/Topic/Source。
    /// </summary>
    /// <param name="defaultPubSubName">默认 PubSub 名称</param>
    /// <param name="defaultTopic">默认 Topic</param>
    /// <param name="defaultSource">默认 Source</param>
    public Registry(string defaultPubSubName, string defaultTopic, string defaultSource)
    {
        _defaultPubSubName = defaultPubSubName;
        _defaultTopic = defaultTopic;
        _defaultSource = defaultSource;
    }

    /// <summary>
    /// 构建注册中心，将所有委托转为处理器实例。
    /// </summary>
    /// <param name="services">依赖注入容器</param>
    /// <returns>自身</returns>
    internal Registry Build(IServiceProvider services)
    {
        foreach (var (metadata, handlerDelegate) in _handlerDelegates)
        {
            // 创建 CloudEventHandler 实例并注册
            var handler = ActivatorUtilities.CreateInstance<CloudEventHandler>(services, metadata, handlerDelegate);
            _handlers.TryAdd(metadata, handler);
        }
        return this;
    }

    /// <summary>
    /// 注册事件类型的元数据。
    /// </summary>
    /// <param name="eventDataType">事件类型</param>
    /// <param name="attribute">事件特性</param>
    internal void RegisterMetadata(Type eventDataType, CloudEventAttribute attribute)
    {
        var metadata = new CloudEventMetadata(
            PubSubName: attribute.PubSubName ?? _defaultPubSubName,
            Topic: attribute.Topic ?? _defaultTopic,
            Type: attribute.Type ?? eventDataType.Name,
            Source: attribute.Source ?? _defaultSource
        );
        _metadata.TryAdd(eventDataType, metadata);
    }

    /// <summary>
    /// 获取指定事件类型的元数据。
    /// </summary>
    /// <param name="eventDataType">事件类型</param>
    /// <returns>事件元数据</returns>
    internal CloudEventMetadata GetMetadata(Type eventDataType)
    {
        return _metadata[eventDataType];
    }

    /// <summary>
    /// 查找指定元数据对应的处理器实例。
    /// </summary>
    /// <param name="metadata">事件元数据</param>
    /// <param name="handler">输出：处理器实例</param>
    /// <returns>是否找到</returns>
    internal bool TryGetHandler(CloudEventMetadata metadata, [NotNullWhen(true)] out CloudEventHandler? handler)
    {
        return _handlers.TryGetValue(metadata, out handler);
    }

    /// <summary>
    /// 注册事件处理器委托。
    /// 泛型方法，确保类型安全。
    /// </summary>
    /// <typeparam name="TData">事件数据类型</typeparam>
    /// <param name="metadata">事件元数据</param>
    internal void RegisterHandler<TData>(CloudEventMetadata metadata)
    {
        _handlerDelegates.TryAdd(metadata, Handle);

        // 委托：将原始 CloudEvent 反序列化为强类型后，调用对应的 ICloudEventHandler<TData>
        static Task Handle(IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token)
        {
            var typedEvent = new CloudEvent<TData>(
                Id: @event.Id,
                Source: @event.Source,
                Type: @event.Type,
                Time: @event.Time,
                Data: @event.Data.Deserialize<TData>(JSON.DefaultJsonSerializerOptions)!,
                DataSchema: @event.DataSchema,
                Subject: @event.Subject
            );
            // 调用用户自定义的事件处理器
            return serviceProvider.GetRequiredService<ICloudEventHandler<TData>>().HandleAsync(typedEvent, token);
        }
    }

    /// <summary>
    /// 获取指定 pubsub 下所有已订阅的 topic。
    /// </summary>
    /// <param name="pubSubName">PubSub 名称</param>
    /// <returns>已订阅的 topic 列表</returns>
    public IEnumerable<string> GetSubscribedTopics(string pubSubName)
    {
        return _handlers.Keys
            .Where(m => m.PubSubName == pubSubName)
            .Select(m => m.Topic)
            .Distinct();
    }

    /// <summary>
    /// 调试用：输出已注册的元数据和处理器信息。
    /// </summary>
    /// <returns>调试字符串</returns>
    public string Debug()
    {
        var sb = new StringBuilder();

        sb.AppendLine("Metadata:");
        foreach (var (key, value) in _metadata)
        {
            sb.AppendLine($"{key}: {value}");
        }

        sb.AppendLine("Handlers:");
        foreach (var (key, value) in _handlers)
        {
            sb.AppendLine($"{key}: {value}");
        }

        return sb.ToString();
    }
}
