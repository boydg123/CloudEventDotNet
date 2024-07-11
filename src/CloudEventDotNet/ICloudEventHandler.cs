
namespace CloudEventDotNet;

/// <summary>
/// CloudEvent<typeparamref name="TData"/> ������
/// </summary>
/// <typeparam name="TData">The data type of the registered CloudEvent, it must has a <see cref="CloudEventAttribute"/> attribute</typeparam>
public interface ICloudEventHandler<TData>
{
    /// <summary>
    /// ����CloudEvent<typeparamref name="TData"/>���첽����
    /// </summary>
    /// <param name="cloudEvent">Ҫ�����CloudEvent����</param>
    /// <param name="token">�첽������ȡ�����</param>
    /// <returns></returns>
    Task HandleAsync(CloudEvent<TData> cloudEvent, CancellationToken token);
}
