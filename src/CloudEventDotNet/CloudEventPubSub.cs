using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

/// <summary>
/// ʵ�� <see cref="ICloudEventPubSub"/>
/// </summary>
internal sealed class CloudEventPubSub : ICloudEventPubSub
{
    private readonly PubSubOptions _options;
    private readonly Dictionary<string, ICloudEventPublisher> _publishers;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly Registry _registry;

    public CloudEventPubSub(
        IServiceProvider services,
        Registry registry,
        IOptions<PubSubOptions> options,
        ILoggerFactory loggerFactory)
    {
        _options = options.Value;
        _loggerFactory = loggerFactory;
        _publishers = _options.PublisherFactoris.ToDictionary(kvp => kvp.Key, kvp => kvp.Value(services));
        _logger = loggerFactory.CreateLogger<CloudEventPubSub>();
        _registry = registry;

    }

    /// <summary>
    /// �����¼�
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task PublishAsync<TData>(TData data)
    {
        // ��ȡʱ������
        var dataType = typeof(TData);

        // ��ȡԪ����
        var metadata = _registry.GetMetadata(dataType);

        // ���� CloudEvent
        var cloudEvent = new CloudEvent<TData>(
            Id: Guid.NewGuid().ToString(),
            Source: metadata.Source,
            Type: metadata.Type,
            Time: DateTimeOffset.UtcNow,
            Data: data,
            DataSchema: null,
            Subject: null
        );
        // using var activity = CloudEventPublishTelemetry.OnCloudEventPublishing(metadata, cloudEvent);
        _logger.LogDebug($"Publishing cloud event: {JSON.Serialize(cloudEvent)}, Metadata: {JSON.Serialize(metadata)}");

        // �����¼�
        var publisher = _publishers[metadata.PubSubName];
        await publisher.PublishAsync(metadata.Topic, cloudEvent).ConfigureAwait(false);

        //ConfigureAwait(false) ȷ���첽�����ڵ�ǰ�߳���ִ��
        //false����ʾ�ڵȴ��������ʱ������Ҫ���ص�ԭ����ͬ�������ġ�����ζ���첽�������᳢����ԭʼ�������м���ִ�У��Ӷ�������һЩǱ�ڵ�������������������
        //ʹ�ó���
        //������ܣ��ڲ���Ҫ���ص�ԭʼͬ�������ĵ�����£�ʹ��.ConfigureAwait(false) ���Լ����߳��л��Ŀ���������첽���������ܡ�
        //������������ĳЩ����£������ʹ��.ConfigureAwait(false)���첽�������ܻ��ڵȴ��������ʱ���Է��ص�ԭʼͬ�������ģ�����ܵ���������
        //true ��һЩUI�߳��У���Ҫ���ص���ǰ��UI�߳�

        _logger.LogDebug($"Published CloudEvent:{JSON.Serialize(metadata)}");

        // CloudEventPublishTelemetry.OnCloudEventPublished(metadata);
    }
}
