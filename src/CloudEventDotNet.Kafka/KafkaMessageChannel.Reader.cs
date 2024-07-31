using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 消息通道消息读取器
/// </summary>
internal class KafkaMessageChannelReader
{
    private readonly Task _readLoop; // 用于运行读取循环的任务
    private readonly ChannelReader<KafkaMessageWorkItem> _channelReader;
    private readonly CancellationToken _stopToken;
    private readonly ILogger _logger;

    public KafkaMessageChannelReader(
        ChannelReader<KafkaMessageWorkItem> channelReader,
        ILoggerFactory loggerFactory,
        CancellationToken stopToken
        )
    {
        _channelReader = channelReader;
        _stopToken = stopToken;

        _readLoop = Task.Run(ReadLoop, default); //启动一个后台任务 _readLoop 来运行 ReadLoop 方法
        _logger = loggerFactory.CreateLogger<KafkaMessageChannelReader>();
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
            _logger.LogDebug("Polling started");
            while (true)
            {
                // 从通道中读取 Kafka 消息工作项并处理它们
                if (_channelReader.TryRead(out var workItem))
                {
                    //如果成功读取到工作项，则执行并等待其完成，同时更新偏移量
                    if (!workItem.Started)
                    {
                        _logger.LogTrace("Work item not started, starting it");
                        workItem.Execute();
                        _logger.LogTrace("Work item started");
                    }
                    var vt = workItem.WaitToCompleteAsync();
                    if (!vt.IsCompletedSuccessfully)
                    {
                        _logger.LogTrace("Work item not completed, waiting");
                        await vt.ConfigureAwait(false);
                        _logger.LogTrace("Work item completed");
                    }
                    Offset = workItem.TopicPartitionOffset;
                    _logger.LogTrace($"Checked offset {Offset.Offset.Value}");
                }
                else
                {
                    //如果没有读取到工作项，检查取消令牌是否被请求，如果是则停止循环，否则等待下一个工作项
                    if (_stopToken.IsCancellationRequested)
                    {
                        _logger.LogDebug("Reader stopped");
                        return;
                    }
                    else
                    {
                        _logger.LogTrace("Waiting for next work item");
                        await _channelReader.WaitToReadAsync(_stopToken).ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _stopToken)
        {
            _logger.LogTrace("Reader cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception on polling");
        }
        _logger.LogDebug("Reader stopped");
    }
}

