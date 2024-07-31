using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis∂©‘ƒ µœ÷ <see cref="ICloudEventSubscriber"/>.
/// </summary>
internal class RedisCloudEventSubscriber : ICloudEventSubscriber
{
    private readonly string _pubSubName;
    private readonly ILogger _logger;
    private readonly RedisMessageChannelFactory _channelFactory;
    private RedisMessageChannel[]? _subscribers;

    public RedisCloudEventSubscriber(
        string pubSubName,
        ILogger<RedisCloudEventSubscriber> logger,
        RedisMessageChannelFactory channelFactory)
    {
        _pubSubName = pubSubName;
        _logger = logger;
        _channelFactory = channelFactory;
    }

    public Task StartAsync()
    {
        _logger.LogDebug("Subscribe starting");
        _subscribers = _channelFactory.Create(_pubSubName);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        await Task.WhenAll(_subscribers!.Select(s => s.StopAsync()));
    }
}
