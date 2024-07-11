using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

/// <summary>
/// 实现 <see cref="ICloudEventPubSub"/>
/// </summary>
internal sealed class CloudEventPubSub : ICloudEventPubSub
{
    private readonly PubSubOptions _options;
    private readonly Dictionary<string, ICloudEventPublisher> _publishers;
    private readonly ILogger<CloudEventPubSub> _logger;
    private readonly Registry _registry;

    public CloudEventPubSub(
        ILogger<CloudEventPubSub> logger,
        IServiceProvider services,
        Registry registry,
        IOptions<PubSubOptions> options)
    {
        _options = options.Value;
        _publishers = _options.PublisherFactoris.ToDictionary(kvp => kvp.Key, kvp => kvp.Value(services));
        _logger = logger;
        _registry = registry;
    }

    /// <summary>
    /// 发布事件
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task PublishAsync<TData>(TData data)
    {
        // 获取时间类型
        var dataType = typeof(TData);

        // 获取元数据
        var metadata = _registry.GetMetadata(dataType);

        // 构建 CloudEvent
        var cloudEvent = new CloudEvent<TData>(
            Id: Guid.NewGuid().ToString(),
            Source: metadata.Source,
            Type: metadata.Type,
            Time: DateTimeOffset.UtcNow,
            Data: data,
            DataSchema: null,
            Subject: null
        );
        using var activity = CloudEventPublishTelemetry.OnCloudEventPublishing(metadata, cloudEvent);

        // 发布事件
        var publisher = _publishers[metadata.PubSubName];
        await publisher.PublishAsync(metadata.Topic, cloudEvent).ConfigureAwait(false);

        //ConfigureAwait(false) 确保异步方法在当前线程中执行
        //false：表示在等待任务完成时，不需要返回到原来的同步上下文。这意味着异步操作不会尝试在原始上下文中继续执行，从而避免了一些潜在的性能问题和死锁情况。
        //使用场景
        //提高性能：在不需要返回到原始同步上下文的情况下，使用.ConfigureAwait(false) 可以减少线程切换的开销，提高异步操作的性能。
        //避免死锁：在某些情况下，如果不使用.ConfigureAwait(false)，异步方法可能会在等待任务完成时尝试返回到原始同步上下文，这可能导致死锁。
        //true 在一些UI线程中，需要返回到当前的UI线程

        CloudEventPublishTelemetry.OnCloudEventPublished(metadata);
    }
}
