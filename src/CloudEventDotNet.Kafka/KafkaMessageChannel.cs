using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka消息通道
/// </summary>
internal class KafkaMessageChannel
{
    private readonly Channel<KafkaMessageWorkItem> _channel; //用于存储 Kafka 消息工作项的通道
    private readonly CancellationTokenSource _stopTokenSource = new(); //用于停止消息通道的 CancellationTokenSource
    private readonly KafkaMessageChannelContext _channelContext; //消息通道上下文
    private readonly KafkaWorkItemContext _workItemContext; //工作项上下文
    private readonly KafkaMessageChannelTelemetry _telemetry; 

    public KafkaMessageChannel(
        KafkaSubscribeOptions options,
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext workItemContext,
        KafkaMessageChannelTelemetry telemetry)
    {
        if (options.RunningWorkItemLimit > 0)
        {
            // 创建有界(有容量)通道
            _channel = Channel.CreateBounded<KafkaMessageWorkItem>(new BoundedChannelOptions(options.RunningWorkItemLimit)
            {
                SingleReader = true,
                SingleWriter = true
            });
            telemetry.Logger.LogDebug("Created bounded channel");
        }
        else
        {
            // 创建无界(无容量)通道 - 可能会耗尽内存资源
            _channel = Channel.CreateUnbounded<KafkaMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            telemetry.Logger.LogDebug("Created unbounded channel");
        }

        Reader = new KafkaMessageChannelReader(
            _channel.Reader,
            telemetry,
            _stopTokenSource.Token);
        _channelContext = channelContext;
        _workItemContext = workItemContext;
        _telemetry = telemetry;
    }

    public bool IsActive { get; }

    /// <summary>
    /// 停止写入
    /// </summary>
    /// <returns></returns>
    public Task StopAsync()
    {
        _stopTokenSource.Cancel();
        return Reader.StopAsync();
    }

    /// <summary>
    /// 写入消息到Channel
    /// </summary>
    /// <param name="message"></param>
    public void DispatchMessage(ConsumeResult<byte[], byte[]> message)
    {
        var workItem = new KafkaMessageWorkItem(
            _channelContext,
            _workItemContext,
            _telemetry,
            message);

        if (!_channel.Writer.TryWrite(workItem))
        {
            _channel.Writer.WriteAsync(workItem).AsTask().GetAwaiter().GetResult();
        }
        //ThreadPool.UnsafeQueueUserWorkItem(workItem, false);
    }

    public KafkaMessageChannelReader Reader { get; }
}
