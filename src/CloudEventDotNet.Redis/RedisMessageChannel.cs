using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal sealed partial class RedisMessageChannel
{
    private readonly Channel<RedisMessageWorkItem> _channel;
    private readonly CancellationTokenSource _stopTokenSource = new();
    private readonly ILogger _logger;
    //private readonly ILoggerFactory _loggerFactory;

    public RedisMessageChannel(
        RedisSubscribeOptions options,
        IDatabase database,
        RedisMessageChannelContext channelContext,
        RedisWorkItemContext workItemContext,
        ILoggerFactory loggerFactory)
    {
        //_loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<RedisMessageChannel>();

        int capacity = options.RunningWorkItemLimit;
        if (capacity > 0)
        {
            _channel = Channel.CreateBounded<RedisMessageWorkItem>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created bounded channel");
        }
        else
        {
            _channel = Channel.CreateUnbounded<RedisMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created unbounded channel");
        }

        Writer = new RedisMessageChannelWriter(
            options,
            database,
            channelContext,
            _channel.Writer,
            workItemContext,
            loggerFactory,
            _stopTokenSource.Token);

        Reader = new RedisMessageChannelReader(
            _channel.Reader,
            loggerFactory,
            _stopTokenSource.Token);
    }

    public RedisMessageChannelWriter Writer { get; }
    public RedisMessageChannelReader Reader { get; }

    public async Task StopAsync()
    {
        _stopTokenSource.Cancel();
        await Writer.StopAsync();
        await Reader.StopAsync();
    }
}
