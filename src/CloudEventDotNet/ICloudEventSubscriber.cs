namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 订阅者接口。
/// 用于实现具体的消息中间件订阅逻辑（如Kafka、Redis等）。
/// </summary>
public interface ICloudEventSubscriber
{
    /// <summary>
    /// 启动订阅。
    /// </summary>
    /// <returns>异步任务</returns>
    Task StartAsync();
    /// <summary>
    /// 停止订阅。
    /// </summary>
    /// <returns>异步任务</returns>
    Task StopAsync();
}
