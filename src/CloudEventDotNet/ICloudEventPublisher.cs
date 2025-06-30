namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 发布者接口。
/// 用于将 CloudEvent 事件发布到指定的消息主题（如Kafka、Redis等）。
/// 支持泛型事件数据类型，便于类型安全和扩展。
/// </summary>
public interface ICloudEventPublisher
{
    /// <summary>
    /// 发布 CloudEvent。
    /// </summary>
    /// <typeparam name="TData">事件数据类型</typeparam>
    /// <param name="topic">消息主题</param>
    /// <param name="cloudEvent">要发布的 CloudEvent 实例</param>
    /// <returns>异步任务</returns>
    Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent);
}
