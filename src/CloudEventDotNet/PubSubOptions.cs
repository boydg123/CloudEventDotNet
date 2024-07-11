namespace CloudEventDotNet;

public class PubSubOptions
{
    public Dictionary<string, Func<IServiceProvider, ICloudEventPublisher>> PublisherFactoris { get; set; } = new(); // = new(); 表示当实例化 PubSubOptions 类时，这个字典会被初始化为空字典。
    public Dictionary<string, Func<IServiceProvider, ICloudEventSubscriber>> SubscriberFactoris { get; set; } = new();
}
