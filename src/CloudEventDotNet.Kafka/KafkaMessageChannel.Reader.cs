using System.Threading.Channels;
using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 消息通道消息读取器
/// </summary>
internal class KafkaMessageChannelReader
{
    private readonly Task _readLoop; // 用于运行读取循环的任务
    private readonly ChannelReader<KafkaMessageWorkItem> _channelReader;
    private readonly KafkaMessageChannelTelemetry _telemetry;
    private readonly CancellationToken _stopToken;

    public KafkaMessageChannelReader(
        ChannelReader<KafkaMessageWorkItem> channelReader,
        KafkaMessageChannelTelemetry telemetry,
        CancellationToken stopToken)
    {
        _channelReader = channelReader;
        _telemetry = telemetry;
        _stopToken = stopToken;

        _readLoop = Task.Run(ReadLoop, default); //启动一个后台任务 _readLoop 来运行 ReadLoop 方法
    }

    // 用于存储当前处理的 Kafka 消息的偏移量
    public TopicPartitionOffset? Offset { get; private set; }

    public async Task StopAsync()
    {
        await _readLoop;
    }

    private async Task ReadLoop()
    {
        try
        {
            _telemetry.OnMessageChannelReaderStarted();
            while (true)
            {
                // 从通道中读取 Kafka 消息工作项并处理它们
                if (_channelReader.TryRead(out var workItem))
                {
                    //如果成功读取到工作项，则执行并等待其完成，同时更新偏移量
                    if (!workItem.Started)
                    {
                        _telemetry.OnWorkItemStarting();
                        workItem.Execute();
                        _telemetry.OnWorkItemStarted();
                    }
                    var vt = workItem.WaitToCompleteAsync();
                    if (!vt.IsCompletedSuccessfully)
                    {
                        _telemetry.OnWaitingWorkItemComplete();
                        await vt.ConfigureAwait(false);
                        _telemetry.OnWorkItemCompleted();
                    }
                    Offset = workItem.TopicPartitionOffset;
                    _telemetry.OnOffsetChecked(Offset.Offset.Value);
                }
                else
                {
                    //如果没有读取到工作项，检查取消令牌是否被请求，如果是则停止循环，否则等待下一个工作项
                    if (_stopToken.IsCancellationRequested)
                    {
                        _telemetry.MessageChannelReaderStopped();
                        return;
                    }
                    else
                    {
                        _telemetry.WaitingForNextWorkItem();
                        await _channelReader.WaitToReadAsync(_stopToken).ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _stopToken)
        {
            _telemetry.MessageChannelReaderCancelled();
        }
        catch (Exception ex)
        {
            _telemetry.ExceptionOnReadingWorkItems(ex);
        }
        _telemetry.MessageChannelReaderStopped();
    }
}

