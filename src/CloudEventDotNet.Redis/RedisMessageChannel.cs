using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis 消息通道。
/// 封装了消息的内存 Channel、写入器和读取器，负责消息的拉取、分发和处理。
/// </summary>
internal sealed partial class RedisMessageChannel
{
    /// <summary>
    /// 内存 Channel，用于在拉取和处理之间缓存 WorkItem。
    /// </summary>
    private readonly Channel<RedisMessageWorkItem> _channel;
    /// <summary>
    /// 停止信号源。
    /// </summary>
    private readonly CancellationTokenSource _stopTokenSource = new();
    /// <summary>
    /// 日志记录器。
    /// </summary>
    private readonly ILogger _logger;
    //private readonly ILoggerFactory _loggerFactory;

    /// <summary>
    /// 构造函数，初始化通道、写入器和读取器。
    /// </summary>
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

    /// <summary>
    /// 消息写入器，负责拉取和分发消息。
    /// </summary>
    public RedisMessageChannelWriter Writer { get; }
    /// <summary>
    /// 消息读取器，负责处理消息。
    /// </summary>
    public RedisMessageChannelReader Reader { get; }

    /// <summary>
    /// 停止通道，优雅关闭写入器和读取器。
    /// </summary>
    public async Task StopAsync()
    {
        _stopTokenSource.Cancel();
        await Writer.StopAsync();
        await Reader.StopAsync();
    }
}
