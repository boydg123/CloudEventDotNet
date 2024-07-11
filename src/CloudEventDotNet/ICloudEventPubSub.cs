namespace CloudEventDotNet;

/// <summary>
/// CloudEvents�ķ����ߡ�
/// </summary>
public interface ICloudEventPubSub
{
    /// <summary>
    /// ʹ��ע���Ԫ���ݷ���CloudEvent
    /// </summary>
    /// <typeparam name="TData">CloudEvent���������ͣ�������Ϊ <see cref="CloudEventAttribute"/></typeparam>
    /// <param name="data">CloudEvent����</param>
    /// <returns>The <see cref="Task"/> that represents the asynchronous operation.</returns>
    Task PublishAsync<TData>(TData data);
}
