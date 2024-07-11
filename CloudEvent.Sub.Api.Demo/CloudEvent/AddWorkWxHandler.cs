using System.Text.Json;
using CloudEventDotNet;

namespace CloudEvent.Sub.Api.Demo.CloudEvent;

public class AddWorkWxHandler : ICloudEventHandler<AddWorkWxEvent>
{
    private readonly ILogger _logger;

    public AddWorkWxHandler(ILogger<AddWorkWxHandler> logger) => _logger = logger;

    public Task HandleAsync(CloudEvent<AddWorkWxEvent> cloudEvent, CancellationToken token)
    {
        _logger.LogInformation("Received AddWorkWx event: " + JsonSerializer.Serialize(cloudEvent.Data));
        return Task.CompletedTask;
    }
}
