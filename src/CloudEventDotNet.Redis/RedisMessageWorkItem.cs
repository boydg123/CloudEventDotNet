using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis 消息处理工作项。
/// 封装了单条 Redis Stream 消息的处理、确认、异常处理等逻辑。
/// </summary>
internal sealed class RedisMessageWorkItem : IThreadPoolWorkItem
{
    /// <summary>
    /// 用于取消消息处理的 Token。
    /// </summary>
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    /// <summary>
    /// 用于等待工作项完成的同步器。
    /// </summary>
    private readonly WorkItemWaiter _waiter = new();
    /// <summary>
    /// Redis 工作项相关的上下文信息。
    /// </summary>
    private readonly RedisWorkItemContext _context;
    /// <summary>
    /// 日志记录器。
    /// </summary>
    private readonly ILogger _logger;

    /// <summary>
    /// 构造函数，初始化消息工作项。
    /// </summary>
    /// <param name="channelContext">通道上下文</param>
    /// <param name="context">工作项上下文</param>
    /// <param name="message">Redis Stream 消息</param>
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

    /// <summary>
    /// Redis 消息通道上下文。
    /// </summary>
    public RedisMessageChannelContext ChannelContext { get; }
    /// <summary>
    /// Redis Stream 消息。
    /// </summary>
    public StreamEntry Message { get; }

    /// <summary>
    /// 检查工作项是否已经开始执行。
    /// </summary>
    public bool Started => _started == 1;
    private int _started = 0;

    /// <summary>
    /// 启动消息处理（线程安全，防止重复执行）。
    /// </summary>
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
    /// 用于异步等待工作项完成。
    /// </summary>
    /// <returns>等待任务</returns>
    public ValueTask WaitToCompleteAsync()
    {
        return _waiter.Task;
    }

    /// <summary>
    /// 实际的异步消息处理逻辑，包括反序列化、查找处理器、业务处理、确认消息和异常处理。
    /// </summary>
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
            bool succeed = await handler.HandleAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            if (succeed)
            {
                // 如果处理成功，确认消息已被处理
                await _context.Redis.StreamAcknowledgeAsync(
                    ChannelContext.Topic,
                    ChannelContext.ConsumerGroup,
                    Message.Id).ConfigureAwait(false);
                _logger.LogDebug($"Message {Message.Id} acknowledged");
            }
            //RedisTelemetry.OnMessageProcessed(ChannelContext.ConsumerGroup, ChannelContext.ConsumerName);
            _logger.LogDebug($"Message ConsumerGroup-{ChannelContext.ConsumerGroup},ConsumerName-{ChannelContext.ConsumerName} Processed.");
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
