using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Redis;
/// <summary>
/// Redis 消息通道读取器。
/// 负责从内存 Channel 中读取 WorkItem，并异步执行消息处理。
/// </summary>
internal sealed partial class RedisMessageChannelReader
{
    /// <summary>
    /// 内存 Channel 的读取端。
    /// </summary>
    private readonly ChannelReader<RedisMessageWorkItem> _channelReader;
    /// <summary>
    /// 停止信号 Token。
    /// </summary>
    private readonly CancellationToken _stopToken;
    /// <summary>
    /// 读取循环任务。
    /// </summary>
    private readonly Task _readLoop;
    //private readonly ILoggerFactory _loggerFactory;
    /// <summary>
    /// 日志记录器。
    /// </summary>
    private readonly ILogger _logger;

    // 该代码定义了一个名为 RedisMessageChannelReader 的类，主要用于从通道中读取 RedisMessageWorkItem 对象并处理它们。其主要功能包括：
    // 从通道中读取工作项并执行。
    // 记录遥测信息以监控和调试。
    // 支持取消操作，能够在收到取消请求时优雅地停止。
    // 处理异常情况，确保系统的稳定性。

    /// <summary>
    /// 构造函数，初始化读取器。
    /// </summary>
    public RedisMessageChannelReader(
        ChannelReader<RedisMessageWorkItem> channelReader,
        ILoggerFactory loggerFactory,
        CancellationToken stopToken
        )
    {
        _channelReader = channelReader;
        _stopToken = stopToken;
        _readLoop = Task.Run(ReadLoop, default);
        // _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<RedisMessageChannelReader>();
    }

    /// <summary>
    /// 停止读取循环。
    /// </summary>
    public Task StopAsync()
    {
        return _readLoop;
    }

    /// <summary>
    /// 核心读取循环，不断从 Channel 取出 WorkItem 并执行。
    /// </summary>
    private async Task ReadLoop()
    {
        try
        {
            _logger.LogInformation("Polling started.");
            while (true)
            {
                // 优先尝试读取 WorkItem
                if (_channelReader.TryRead(out var workItem))
                {
                    if (!workItem.Started)
                    {
                        _logger.LogTrace("Work item not started, starting it");
                        workItem.Execute();
                        _logger.LogTrace("Work item started");
                    }
                    // 等待 WorkItem 完成
                    var vt = workItem.WaitToCompleteAsync();
                    if (!vt.IsCompletedSuccessfully)
                    {
                        _logger.LogTrace("Work item not completed, waiting");
                        await vt.ConfigureAwait(false);
                        _logger.LogTrace("Work item completed");
                    }
                }
                else
                {
                    // Channel 暂无可读 WorkItem，检查是否需要退出
                    if (_stopToken.IsCancellationRequested)
                    {
                        _logger.LogDebug("Reader stopped");
                        return;
                    }
                    else
                    {
                        // 等待下一个 WorkItem 到来
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
