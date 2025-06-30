using CloudEventDotNet;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// CloudEventDotNet 服务注册扩展方法。
/// 提供 AddCloudEvents 扩展，便于在依赖注入容器中注册 CloudEvent 相关服务。
/// </summary>
public static class CloudEventsServiceCollectionExtensions
{
    /// <summary>
    /// 注册 CloudEvents 元数据和处理流程。
    /// 这是 CloudEventDotNet 的入口方法，通常在 Startup/Program 中调用。
    /// </summary>
    /// <param name="services">依赖注入服务集合（IServiceCollection）</param>
    /// <param name="defaultPubSubName">默认的 PubSub 名称（如 kafka、redis 等）</param>
    /// <param name="defaultTopic">默认的消息主题</param>
    /// <param name="defaultSource">默认的事件源</param>
    /// <returns>返回 PubSubBuilder，可继续链式配置和加载事件/处理器</returns>
    public static PubSubBuilder AddCloudEvents(
        this IServiceCollection services,
        string defaultPubSubName = "default",
        string defaultTopic = "default",
        string defaultSource = "default")
    {
        // 创建 PubSubBuilder，后续可继续调用 Load/添加中间件等方法
        return new PubSubBuilder(services, defaultPubSubName, defaultTopic, defaultSource);
    }
}
