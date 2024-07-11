using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

/// <summary>
/// CloudEvent处理器委托
/// </summary>
/// <param name="serviceProvider"></param>
/// <param name="event"></param>
/// <param name="token"></param>
/// <returns></returns>
internal delegate Task HandleCloudEventDelegate(IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token);

/// <summary>
/// CloudEvent元数据和处理程序的注册表
/// </summary>
public sealed class Registry
{
    private readonly Dictionary<Type, CloudEventMetadata> _metadata = new();
    private readonly Dictionary<CloudEventMetadata, HandleCloudEventDelegate> _handlerDelegates = new();
    private readonly Dictionary<CloudEventMetadata, CloudEventHandler> _handlers = new();
    private readonly string _defaultPubSubName;
    private readonly string _defaultTopic;
    private readonly string _defaultSource;

    /// <summary>
    /// Constructor of Registry
    /// </summary>
    /// <param name="defaultPubSubName">The default PubSub name</param>
    /// <param name="defaultTopic">The default topic</param>
    /// <param name="defaultSource">The default source</param>
    public Registry(string defaultPubSubName, string defaultTopic, string defaultSource)
    {
        _defaultPubSubName = defaultPubSubName;
        _defaultTopic = defaultTopic;
        _defaultSource = defaultSource;
    }

    internal Registry Build(IServiceProvider services)
    {
        foreach (var (metadata, handlerDelegate) in _handlerDelegates)
        {
            var handler = ActivatorUtilities.CreateInstance<CloudEventHandler>(services, metadata, handlerDelegate);
            _handlers.TryAdd(metadata, handler);
        }
        return this;
    }

    /// <summary>
    /// 添加元数据
    /// </summary>
    /// <param name="eventDataType"></param>
    /// <param name="attribute"></param>
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
    /// 获取元数据
    /// </summary>
    /// <param name="eventDataType"></param>
    /// <returns></returns>
    internal CloudEventMetadata GetMetadata(Type eventDataType)
    {
        return _metadata[eventDataType];
    }

    /// <summary>
    /// 获取处理程序
    /// </summary>
    /// <param name="metadata"></param>
    /// <param name="handler"></param>
    /// <returns></returns>
    internal bool TryGetHandler(CloudEventMetadata metadata, [NotNullWhen(true)] out CloudEventHandler? handler)
    {
        return _handlers.TryGetValue(metadata, out handler);
    }
    /// <summary>
    /// 注册处理程序
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="metadata"></param>
    internal void RegisterHandler<TData>(CloudEventMetadata metadata)
    {
        _handlerDelegates.TryAdd(metadata, Handle);

        // 执行处理程序
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

            return serviceProvider.GetRequiredService<ICloudEventHandler<TData>>().HandleAsync(typedEvent, token);
        }
    }

    /// <summary>
    /// 获取指定pubsub订阅的主题
    /// </summary>
    /// <param name="pubSubName">The pubsub name</param>
    /// <returns></returns>
    public IEnumerable<string> GetSubscribedTopics(string pubSubName)
    {
        return _handlers.Keys
            .Where(m => m.PubSubName == pubSubName)
            .Select(m => m.Topic)
            .Distinct();
    }

    /// <summary>
    /// Show registered metadata and handlers
    /// </summary>
    /// <returns>Registered metadata and handlers</returns>
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
