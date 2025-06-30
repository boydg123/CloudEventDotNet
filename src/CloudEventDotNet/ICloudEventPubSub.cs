namespace CloudEventDotNet;

/// <summary>
/// CloudEvents的发布者。
/// </summary>
public interface ICloudEventPubSub
{
    /// <summary>
    /// 使用注册的元数据发布CloudEvent
    /// </summary>
    /// <typeparam name="TData">CloudEvent的数据类型，必须标记为 <see cref="CloudEventAttribute"/></typeparam>
    /// <param name="data">CloudEvent数据</param>
    /// <returns>The <see cref="Task"/> that represents the asynchronous operation.</returns>
    Task PublishAsync<TData>(TData data);
}
