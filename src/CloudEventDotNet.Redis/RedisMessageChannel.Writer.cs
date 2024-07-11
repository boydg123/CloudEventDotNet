using System.Threading.Channels;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis消息通道写入器
/// </summary>
internal sealed class RedisMessageChannelWriter
{
    //Redis 消息通道写入器：该类负责从 Redis 数据库中读取新消息和待处理消息，并将这些消息分发到通道中进行处理
    //异步任务管理：通过启动两个异步任务 _pollNewMessagesLoop 和 _claimPendingMessagesLoop，实现对新消息和待处理消息的轮询和认领
    //消息分发：将读取到的消息分发到通道中，并使用线程池处理这些消息。
    //优雅停止：提供 StopAsync 方法，用于等待所有异步任务完成，实现优雅停止。
    //通过这些功能，RedisMessageChannelWriter 类有效地管理了 Redis 消息的读取、分发和处理，确保了系统的稳定性和高效性。

    private readonly RedisSubscribeOptions _options; // Redis订阅选项 
    private readonly IDatabase _database; // Redis数据库
    private readonly RedisMessageChannelContext _channelContext; // Redis消息通道上下文
    private readonly ChannelWriter<RedisMessageWorkItem> _channelWriter; // Redis消息通道写入器
    private readonly RedisWorkItemContext _workItemContext; // Redis工作项上下文
    private readonly RedisMessageTelemetry _telemetry;
    private readonly CancellationToken _stopToken;
    private readonly Task _pollNewMessagesLoop; // 轮询新消息循环
    private readonly Task _claimPendingMessagesLoop; // 领取待处理消息循环

    public RedisMessageChannelWriter(
        RedisSubscribeOptions options,
        IDatabase database,
        RedisMessageChannelContext channelContext,
        ChannelWriter<RedisMessageWorkItem> channelWriter,
        RedisWorkItemContext workItemContext,
        RedisMessageTelemetry telemetry,
        CancellationToken stopToken)
    {
        _options = options;
        _database = database;
        _channelContext = channelContext;
        _channelWriter = channelWriter;
        _workItemContext = workItemContext;
        _telemetry = telemetry;
        _stopToken = stopToken;
        _pollNewMessagesLoop = Task.Run(PollNewMessagesLoop, default);
        _claimPendingMessagesLoop = Task.Run(ClaimPendingMessagesLoop, default);
    }

    public async Task StopAsync()
    {
        await _pollNewMessagesLoop;
        await _claimPendingMessagesLoop;
    }

    /// <summary>
    /// 新消息轮询循环
    /// </summary>
    /// <returns></returns>
    private async Task PollNewMessagesLoop()
    {
        try
        {
            _telemetry.OnFetchMessagesLoopStarted();
            try
            {
                // 创建消费者组，如果消费者组已存在则捕获异常并继续
                await _database.StreamCreateConsumerGroupAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    StreamPosition.NewMessages);
            }
            catch (RedisServerException ex)
            {
                if (ex.Message != "BUSYGROUP Consumer Group name already exists") throw;
            }

            while (!_stopToken.IsCancellationRequested)
            {
                // 循环读取新消息，如果有消息则分发，否则等待一段时间后继续轮询
                var messages = await _database.StreamReadGroupAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    _channelContext.ConsumerGroup,
                    StreamPosition.NewMessages,
                    _options.PollBatchSize).ConfigureAwait(false);

                if (messages.Length > 0)
                {
                    _telemetry.OnMessagesFetched(messages.Length);
                    await DispatchMessages(messages).ConfigureAwait(false);
                }
                else
                {
                    _telemetry.OnNoMessagesFetched();
                    await Task.Delay(_options.PollInterval, default);
                }
            }
            _telemetry.OnFetchNewMessagesLoopStopped();
        }
        catch (Exception ex)
        {
            _telemetry.OnFetchNewMessagesLoopError(ex);
            throw;
        }
    }

    /// <summary>
    /// 领取待处理消息循环
    /// 循环认领待处理消息，如果消息超时则认领并分发，否则等待一段时间后继续轮询
    /// </summary>
    /// <returns></returns>

    private async Task ClaimPendingMessagesLoop()
    {
        try
        {
            _telemetry.OnClaimMessagesLoopStarted();
            while (!_stopToken.IsCancellationRequested)
            {
                await ClaimPendingMessages().ConfigureAwait(false);
                await Task.Delay(_options.PollInterval, default);
            }
            _telemetry.OnClaimMessagesLoopStopped();

            async Task ClaimPendingMessages()
            {
                while (!_stopToken.IsCancellationRequested)
                {
                    var pendingMessages = await _database.StreamPendingMessagesAsync(
                        _channelContext.Topic,
                        _channelContext.ConsumerGroup,
                        _options.PollBatchSize,
                        RedisValue.Null).ConfigureAwait(false);
                    _telemetry.OnPendingMessagesInformationFetched(pendingMessages.Length);

                    if (pendingMessages.Length == 0)
                    {
                        _telemetry.OnNoMessagesToClaim();
                        return;
                    }

                    var messagesToClaim = pendingMessages
                        .Where(msg => msg.IdleTimeInMilliseconds >= _options.ProcessingTimeout.TotalMilliseconds)
                        .Select(msg => msg.MessageId)
                        .ToArray();

                    if (messagesToClaim.Length == 0)
                    {
                        var first = pendingMessages[0];
                        _telemetry.OnNoTimeoutedMessagesToClaim(first.MessageId.ToString(), first.IdleTimeInMilliseconds, first.DeliveryCount);
                        return;
                    }

                    var claimedMessages = await _database.StreamClaimAsync(
                        _channelContext.Topic,
                        _channelContext.ConsumerGroup,
                        _channelContext.ConsumerGroup,
                        (long)_options.ProcessingTimeout.TotalMilliseconds,
                        messagesToClaim
                    ).ConfigureAwait(false);

                    _telemetry.OnMessagesClaimed(claimedMessages.Length);
                    await DispatchMessages(claimedMessages).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            _telemetry.OnClaimMessagesLoopError(ex);
            throw;
        }
    }

    /// <summary>
    /// 负责将消息分发到通道中，并使用线程池处理工作项。
    /// </summary>
    /// <param name="messages"></param>
    /// <returns></returns>
    private async ValueTask DispatchMessages(StreamEntry[] messages)
    {
        foreach (var message in messages)
        {
            var workItem = new RedisMessageWorkItem(
                _channelContext,
                _workItemContext,
                message
            );
            if (!_channelWriter.TryWrite(workItem))
            {
                await _channelWriter.WriteAsync(workItem).ConfigureAwait(false);
            }
            ThreadPool.UnsafeQueueUserWorkItem(workItem, false);
            _telemetry.OnMessageDispatched(message.Id.ToString());
        }
        _telemetry.OnMessagesDispatched(messages.Length);
    }

}
