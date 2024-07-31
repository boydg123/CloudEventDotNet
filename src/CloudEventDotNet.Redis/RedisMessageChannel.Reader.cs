using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Redis;
/// <summary>
/// Redis消息通道读取器
/// </summary>
internal sealed partial class RedisMessageChannelReader
{
    // 该代码定义了一个名为 RedisMessageChannelReader 的类，主要用于从通道中读取 RedisMessageWorkItem 对象并处理它们。其主要功能包括：
    // 从通道中读取工作项并执行。
    // 记录遥测信息以监控和调试。
    // 支持取消操作，能够在收到取消请求时优雅地停止。
    // 处理异常情况，确保系统的稳定性。
    private readonly ChannelReader<RedisMessageWorkItem> _channelReader; // 用于从通道中读取 RedisMessageWorkItem 对象
    private readonly CancellationToken _stopToken;
    private readonly Task _readLoop; // 用于执行读取循环的任务。
    //private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;

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

    public Task StopAsync()
    {
        return _readLoop;
    }

    // 用于等待读取循环的完成
    private async Task ReadLoop()
    {
        try
        {
            _logger.LogInformation("Polling started.");
            while (true) // 进入一个无限循环，尝试从通道中读取工作项
            {
                // 如果成功读取到工作项，检查其是否已启动，如果未启动则执行并记录遥测信息
                if (_channelReader.TryRead(out var workItem))
                {
                    if (!workItem.Started)
                    {
                        _logger.LogTrace("Work item not started, starting it");
                        workItem.Execute();
                        _logger.LogTrace("Work item started");
                    }
                    //等待工作项完成，如果未完成则记录遥测信息并等待其完成
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
                    // 如果未读取到工作项，检查取消令牌是否已请求取消，如果是则记录遥测信息并退出循环。
                    if (_stopToken.IsCancellationRequested)
                    {
                        _logger.LogDebug("Reader stopped");
                        return;
                    }
                    else
                    {
                        // 如果没有取消请求，记录等待下一个工作项的遥测信息并等待通道可读
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
