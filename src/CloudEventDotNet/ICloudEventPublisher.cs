namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 发布者接口.
/// </summary>
public interface ICloudEventPublisher
{
    /// <summary>
    /// 发布 CloudEvent.
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="topic"></param>
    /// <param name="cloudEvent"></param>
    /// <returns></returns>
    Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent);
}
