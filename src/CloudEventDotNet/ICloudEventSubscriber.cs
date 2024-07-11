namespace CloudEventDotNet;

/// <summary>
/// CloudEvent ¶©ÔÄ½Ó¿Ú.
/// </summary>
public interface ICloudEventSubscriber
{
    Task StartAsync();
    Task StopAsync();
}
