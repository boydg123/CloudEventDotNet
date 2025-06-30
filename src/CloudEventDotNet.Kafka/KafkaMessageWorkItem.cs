using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 消息处理工作项。
/// 封装了从 Kafka 拉取到的单个消息，并负责其处理、反序列化、调用事件处理器、失败重发等逻辑。
/// </summary>
internal sealed class KafkaMessageWorkItem : IThreadPoolWorkItem
{
    // 通道上下文
    private readonly KafkaMessageChannelContext _channelContext;
    // 工作项上下文
    private readonly KafkaWorkItemContext _context;
    // 日志实例
    private readonly ILogger _logger;
    // Kafka 原始消息
    private readonly ConsumeResult<byte[], byte[]> _message;
    // 停止令牌源
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    internal KafkaMessageWorkItem(
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext context,
        ILoggerFactory loggerFactory,
        ConsumeResult<byte[], byte[]> message)
    {
        _logger = loggerFactory.CreateLogger<KafkaMessageWorkItem>();
        _channelContext = channelContext;
        _context = context;
        _message = message;
    }

    // 消息的 Kafka 分区偏移量
    public TopicPartitionOffset TopicPartitionOffset => _message.TopicPartitionOffset;

    // 标记工作项是否已开始执行
    public bool Started => _started == 1;
    private int _started = 0;

    /// <summary>
    /// 执行工作项。由线程池调用。
    /// </summary>
    public void Execute()
    {
        // 确保 ExecuteAsync 只被调用一次
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _ = ExecuteAsync();
        }
        else
        {
            return;
        }
    }

    // 用于等待任务完成的等待器
    private readonly WorkItemWaiter _waiter = new();
    /// <summary>
    /// 异步等待工作项处理完成。
    /// </summary>
    /// <returns>ValueTask</returns>
    public ValueTask WaitToCompleteAsync()
    {
        return _waiter.Task;
    }

    /// <summary>
    /// 异步执行消息处理的核心逻辑。
    /// </summary>
    internal async Task ExecuteAsync()
    {
        try
        {
            // 反序列化为通用 CloudEvent
            var cloudEvent = JSON.Deserialize<CloudEvent>(_message.Message.Value)!;
            // 构建元数据
            var metadata = new CloudEventMetadata(_channelContext.PubSubName, _message.Topic, cloudEvent.Type, cloudEvent.Source);
            // 查找对应的事件处理器
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                _logger.LogDebug($"No handler for {metadata}, ignored");
                return;
            }
            // 调用处理器
            bool succeed = await handler.HandleAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            _logger.LogDebug($"messaging.kafka.client_id:{_channelContext.ConsumerName}.messaging.kafka.consumer_group:{_channelContext.ConsumerGroup}");

            if (!succeed)
            {
                // 如果处理失败，则重新发送消息
                await _context.Producer.ReproduceAsync(_message).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling Kafka message");
        }
        finally
        {
            // 标记工作项完成
            _waiter.SetResult();
        }
    }
}

