using CloudEventDotNet;

namespace Microsoft.Extensions.DependencyInjection;


/// <summary>
/// Extensions to scan for CloudEvents and configure them.
/// </summary>
public static class CloudEventsServiceCollectionExtensions
{

    /// <summary>
    /// 注册CloudEvents元数据和处理程序
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="defaultPubSubName">The default PubSub name</param>
    /// <param name="defaultTopic">The default topic</param>
    /// <param name="defaultSource">The default source</param>
    public static PubSubBuilder AddCloudEvents(
        this IServiceCollection services,
        string defaultPubSubName = "default",
        string defaultTopic = "default",
        string defaultSource = "default")
    {
        return new PubSubBuilder(services, defaultPubSubName, defaultTopic, defaultSource);
    }
}
