namespace CloudEventDotNet;

/// <summary>
/// CloudEvent �����߽ӿ�.
/// </summary>
public interface ICloudEventPublisher
{
    /// <summary>
    /// ���� CloudEvent.
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="topic"></param>
    /// <param name="cloudEvent"></param>
    /// <returns></returns>
    Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent);
}
