using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 消息通道读取器。
/// 负责从内存通道（Channel）中读取消息工作项，并异步执行它们。
/// 这是消息处理的核心循环。
/// </summary>
internal class KafkaMessageChannelReader
{
    // 读取循环的后台任务
    private readonly Task _readLoop;
    // 内存通道的读取端
    private readonly ChannelReader<KafkaMessageWorkItem> _channelReader;
    // 停止令牌
    private readonly CancellationToken _stopToken;
    // 日志实例
    private readonly ILogger _logger;

    public KafkaMessageChannelReader(
        ChannelReader<KafkaMessageWorkItem> channelReader,
        ILoggerFactory loggerFactory,
        CancellationToken stopToken
        )
    {
        _channelReader = channelReader;
        _stopToken = stopToken;

        // 启动后台任务来执行读取循环
        _readLoop = Task.Run(ReadLoop, default);
        _logger = loggerFactory.CreateLogger<KafkaMessageChannelReader>();
    }

    /// <summary>
    /// 当前已成功处理的最新消息的偏移量。
    /// </summary>
    public TopicPartitionOffset? Offset { get; private set; }

    /// <summary>
    /// 停止读取器。
    /// </summary>
    public async Task StopAsync()
    {
        await _readLoop;
    }

    /// <summary>
    /// 读取循环。
    /// 持续从通道中读取工作项，执行并等待其完成，然后更新偏移量。
    /// </summary>
    private async Task ReadLoop()
    {
        try
        {
            _logger.LogDebug("Polling started");
            while (true)
            {
                // 尝试从通道中读取一个工作项
                if (_channelReader.TryRead(out var workItem))
                {
                    // 如果工作项尚未启动，则启动它
                    if (!workItem.Started)
                    {
                        _logger.LogTrace("Work item not started, starting it");
                        workItem.Execute();
                        _logger.LogTrace("Work item started");
                    }
                    // 异步等待工作项处理完成
                    var vt = workItem.WaitToCompleteAsync();
                    if (!vt.IsCompletedSuccessfully)
                    {
                        _logger.LogTrace("Work item not completed, waiting");
                        await vt.ConfigureAwait(false);
                        _logger.LogTrace("Work item completed");
                    }
                    // 更新已处理的偏移量
                    Offset = workItem.TopicPartitionOffset;
                    _logger.LogTrace($"Checked offset {Offset.Offset.Value}");
                }
                else
                {
                    // 如果通道为空，检查是否已请求停止
                    if (_stopToken.IsCancellationRequested)
                    {
                        _logger.LogDebug("Reader stopped");
                        return;
                    }
                    else
                    {
                        // 等待下一个工作项可读
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

