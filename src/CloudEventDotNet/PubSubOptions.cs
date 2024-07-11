namespace CloudEventDotNet;

public class PubSubOptions
{
    public Dictionary<string, Func<IServiceProvider, ICloudEventPublisher>> PublisherFactoris { get; set; } = new(); // = new(); ��ʾ��ʵ���� PubSubOptions ��ʱ������ֵ�ᱻ��ʼ��Ϊ���ֵ䡣
    public Dictionary<string, Func<IServiceProvider, ICloudEventSubscriber>> SubscriberFactoris { get; set; } = new();
}
