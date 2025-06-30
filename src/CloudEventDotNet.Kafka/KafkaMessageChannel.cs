using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 消息通道。
/// 负责接收来自消费者的消息，并将其放入一个内存通道（Channel）中，
/// 以便进行异步、有序、并发控制的消息处理。
/// 每个 Kafka 分区对应一个消息通道实例。
/// </summary>
internal class KafkaMessageChannel
{
    // 内存通道，用于存储待处理的消息工作项
    private readonly Channel<KafkaMessageWorkItem> _channel;
    // 停止令牌源，用于优雅关闭
    private readonly CancellationTokenSource _stopTokenSource = new();
    // 通道上下文信息
    private readonly KafkaMessageChannelContext _channelContext;
    // 工作项上下文信息
    private readonly KafkaWorkItemContext _workItemContext;
    // 日志工厂
    private readonly ILoggerFactory _loggerFactory;
    // 日志实例
    private readonly ILogger _logger;

    public KafkaMessageChannel(
        KafkaSubscribeOptions options,
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext workItemContext,
        ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KafkaMessageChannel>();
        // 根据配置创建有界或无界通道
        if (options.RunningWorkItemLimit > 0)
        {
            // 有界通道，用于限制并发处理的消息数量
            _channel = Channel.CreateBounded<KafkaMessageWorkItem>(new BoundedChannelOptions(options.RunningWorkItemLimit)
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created bounded channel");
        }
        else
        {
            // 无界通道，不限制并发（可能消耗较多内存）
            _channel = Channel.CreateUnbounded<KafkaMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created unbounded channel");
        }

        Reader = new KafkaMessageChannelReader(
            _channel.Reader,
            loggerFactory,
            _stopTokenSource.Token);
        _channelContext = channelContext;
        _workItemContext = workItemContext;
    }

    /// <summary>
    /// 通道是否活动。
    /// </summary>
    public bool IsActive { get; }

    /// <summary>
    /// 停止通道。
    /// </summary>
    /// <returns>异步任务</returns>
    public Task StopAsync()
    {
        _stopTokenSource.Cancel();
        return Reader.StopAsync();
    }

    /// <summary>
    /// 将 Kafka 消息分发到通道中。
    /// </summary>
    /// <param name="message">从 Kafka 拉取到的原始消息</param>
    public void DispatchMessage(ConsumeResult<byte[], byte[]> message)
    {
        var workItem = new KafkaMessageWorkItem(
            _channelContext,
            _workItemContext,
            _loggerFactory,
            message);

        // 尝试非阻塞写入，失败则异步阻塞写入
        if (!_channel.Writer.TryWrite(workItem))
        {
            _channel.Writer.WriteAsync(workItem).AsTask().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// 通道读取器。
    /// </summary>
    public KafkaMessageChannelReader Reader { get; }
}
