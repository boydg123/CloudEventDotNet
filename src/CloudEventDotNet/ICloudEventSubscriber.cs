namespace CloudEventDotNet;

/// <summary>
/// CloudEvent ���Ľӿ�.
/// </summary>
public interface ICloudEventSubscriber
{
    Task StartAsync();
    Task StopAsync();
}
