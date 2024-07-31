using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;
/// <summary>
/// CloudEvent´¦ÀíÆ÷
/// </summary>
internal class CloudEventHandler
{
    private readonly HandleCloudEventDelegate _process;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;

    public CloudEventHandler(
        CloudEventMetadata metadata,
        HandleCloudEventDelegate handleDelegate,
        ILoggerFactory loggerFactory,
        IServiceScopeFactory scopeFactory)
    {
        _process = handleDelegate;
        _scopeFactory = scopeFactory;
        _logger = loggerFactory.CreateLogger<CloudEventHandler>();
        //_telemetry = new CloudEventProcessingTelemetry(loggerFactory, metadata);
    }

    public async Task<bool> ProcessAsync(CloudEvent @event, CancellationToken token)
    {
        // using var activity = _telemetry.OnProcessing(@event);
        _logger.LogDebug($"OnProcessing:{JSON.Serialize(@event)}");
        try
        {
            using var scope = _scopeFactory.CreateScope();
            await _process(scope.ServiceProvider, @event, token).ConfigureAwait(false);
            _logger.LogDebug($"Process CloudEvent {@event.Id}");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to process CloudEvent {@event.Id}");
            return false;
        }
    }
}
