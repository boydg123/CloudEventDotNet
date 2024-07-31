using System.Text.Json;
using CloudEventDotNet.Redis.Instruments;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis消息处理工作项
/// </summary>
internal sealed class RedisMessageWorkItem : IThreadPoolWorkItem
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly WorkItemWaiter _waiter = new(); // 用于等待工作项完成
    private readonly RedisWorkItemContext _context; // Redis 工作项相关的上下文信息
    private readonly ILogger _logger;

    internal RedisMessageWorkItem(
        RedisMessageChannelContext channelContext,
        RedisWorkItemContext context,
        StreamEntry message)
    {
        _context = context;
        ChannelContext = channelContext;
        Message = message;
        _logger = context.LoggerFactory.CreateLogger<RedisMessageWorkItem>();
    }

    // Redis消息通道上下文
    public RedisMessageChannelContext ChannelContext { get; }
    // Redis工作项上下文
    public StreamEntry Message { get; }

    public bool Started => _started == 1; // 用于检查工作项是否已经开始执行
    private int _started = 0;

    public void Execute()
    {
        // 防止重复执行
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _ = ExecuteAsync();
        }
        else
        {
            return;
        }
    }

    /// <summary>
    /// 用于异步等待工作项完成
    /// </summary>
    /// <returns></returns>
    public ValueTask WaitToCompleteAsync()
    {
        return _waiter.Task;
    }

    internal async Task ExecuteAsync()
    {
        try
        {
            var cloudEvent = JSON.Deserialize<CloudEvent>((byte[])Message["data"]!)!;
            var metadata = new CloudEventMetadata(ChannelContext.PubSubName, ChannelContext.Topic, cloudEvent.Type, cloudEvent.Source);
            // 尝试从注册表中获取处理程序
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                return;
            }
            // 处理程序
            bool succeed = await handler.ProcessAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            if (succeed)
            {
                // 如果处理成功，确认消息已被处理
                await _context.Redis.StreamAcknowledgeAsync(
                    ChannelContext.Topic,
                    ChannelContext.ConsumerGroup,
                    Message.Id).ConfigureAwait(false);
                _logger.LogDebug($"Message {Message.Id} acknowledged");
            }
            RedisTelemetry.OnMessageProcessed(ChannelContext.ConsumerGroup, ChannelContext.ConsumerName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to process message {Message.Id}");
        }
        finally
        {
            _waiter.SetResult();
        }
    }
}
