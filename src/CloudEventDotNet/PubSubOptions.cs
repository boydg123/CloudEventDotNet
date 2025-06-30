namespace CloudEventDotNet;

/// <summary>
/// PubSub 配置选项。
/// 用于注册和管理所有可用的事件发布者和订阅者工厂。
/// 通过字典支持多种消息中间件（如Kafka、Redis等）的动态扩展。
/// </summary>
public class PubSubOptions
{
    /// <summary>
    /// 发布者工厂字典。
    /// key为PubSub名称（如"kafka"、"redis"），value为发布者工厂方法（依赖注入容器->发布者实例）。
    /// </summary>
    public Dictionary<string, Func<IServiceProvider, ICloudEventPublisher>> PublisherFactoris { get; set; } = new(); 

    /// <summary>
    /// 订阅者工厂字典。
    /// key为PubSub名称，value为订阅者工厂方法（依赖注入容器->订阅者实例）。
    /// </summary>
    public Dictionary<string, Func<IServiceProvider, ICloudEventSubscriber>> SubscriberFactoris { get; set; } = new();
}
