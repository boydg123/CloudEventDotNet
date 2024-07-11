
namespace CloudEventDotNet;

/// <summary>
/// CloudEvent<typeparamref name="TData"/> 处理器
/// </summary>
/// <typeparam name="TData">The data type of the registered CloudEvent, it must has a <see cref="CloudEventAttribute"/> attribute</typeparam>
public interface ICloudEventHandler<TData>
{
    /// <summary>
    /// 处理CloudEvent<typeparamref name="TData"/>的异步方法
    /// </summary>
    /// <param name="cloudEvent">要处理的CloudEvent对象</param>
    /// <param name="token">异步操作的取消标记</param>
    /// <returns></returns>
    Task HandleAsync(CloudEvent<TData> cloudEvent, CancellationToken token);
}
