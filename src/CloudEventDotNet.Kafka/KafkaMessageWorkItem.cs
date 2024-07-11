using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka消息处理工作项
/// </summary>
internal sealed class KafkaMessageWorkItem : IThreadPoolWorkItem
{
    private readonly KafkaMessageChannelContext _channelContext; // 消息通道
    private readonly KafkaWorkItemContext _context; // 
    private readonly KafkaMessageChannelTelemetry _telemetry;
    private readonly ConsumeResult<byte[], byte[]> _message; // 消息
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    internal KafkaMessageWorkItem(
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext context,
        KafkaMessageChannelTelemetry telemetry,
        ConsumeResult<byte[], byte[]> message)
    {
        _channelContext = channelContext;
        _context = context;
        _telemetry = telemetry;
        _message = message;
    }

    // 与消息关联的主题分区偏移量
    public TopicPartitionOffset TopicPartitionOffset => _message.TopicPartitionOffset;

    public bool Started => _started == 1; // 用于检查消息处理是否已经开始
    private int _started = 0;

    public void Execute()
    {
        // 确保ExecuteAsync只被调用一次
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _ = ExecuteAsync();
        }
        else
        {
            return;
        }
    }

    private readonly WorkItemWaiter _waiter = new();
    /// <summary>
    /// 用于等待异步操作完成
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
            var cloudEvent = JSON.Deserialize<CloudEvent>(_message.Message.Value)!;
            var metadata = new CloudEventMetadata(_channelContext.PubSubName, _message.Topic, cloudEvent.Type, cloudEvent.Source);
            // 尝试从注册表中获取处理程序
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                CloudEventProcessingTelemetry.OnHandlerNotFound(_telemetry.Logger, metadata);
                return;
            }
            // 如果找到处理程序，异步处理消息。
            bool succeed = await handler.ProcessAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            KafkaConsumerTelemetry.OnConsumed(_channelContext.ConsumerName, _channelContext.ConsumerGroup);

            if (!succeed)
            {
                // 如果处理失败，重新发送消息
                await _context.Producer.ReproduceAsync(_message).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _telemetry.Logger.LogError(ex, "Error handling Kafka message");
        }
        finally
        {
            _waiter.SetResult();
        }
    }
}

