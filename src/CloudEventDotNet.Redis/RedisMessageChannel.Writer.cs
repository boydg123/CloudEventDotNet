using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis��Ϣͨ��д����
/// </summary>
internal sealed class RedisMessageChannelWriter
{
    //Redis ��Ϣͨ��д���������ฺ��� Redis ���ݿ��ж�ȡ����Ϣ�ʹ�������Ϣ��������Щ��Ϣ�ַ���ͨ���н��д���
    //�첽�������ͨ�����������첽���� _pollNewMessagesLoop �� _claimPendingMessagesLoop��ʵ�ֶ�����Ϣ�ʹ�������Ϣ����ѯ������
    //��Ϣ�ַ�������ȡ������Ϣ�ַ���ͨ���У���ʹ���̳߳ش�����Щ��Ϣ��
    //����ֹͣ���ṩ StopAsync ���������ڵȴ������첽������ɣ�ʵ������ֹͣ��
    //ͨ����Щ���ܣ�RedisMessageChannelWriter ����Ч�ع����� Redis ��Ϣ�Ķ�ȡ���ַ��ʹ���ȷ����ϵͳ���ȶ��Ժ͸�Ч�ԡ�

    private readonly RedisSubscribeOptions _options; // Redis����ѡ�� 
    private readonly IDatabase _database; // Redis���ݿ�
    private readonly RedisMessageChannelContext _channelContext; // Redis��Ϣͨ��������
    private readonly ChannelWriter<RedisMessageWorkItem> _channelWriter; // Redis��Ϣͨ��д����
    private readonly RedisWorkItemContext _workItemContext; // Redis������������
    private readonly ILogger _logger;
    private readonly CancellationToken _stopToken;
    private readonly Task _pollNewMessagesLoop; // ��ѯ����Ϣѭ��
    private readonly Task _claimPendingMessagesLoop; // ��ȡ��������Ϣѭ��

    public RedisMessageChannelWriter(
        RedisSubscribeOptions options,
        IDatabase database,
        RedisMessageChannelContext channelContext,
        ChannelWriter<RedisMessageWorkItem> channelWriter,
        RedisWorkItemContext workItemContext,
        ILoggerFactory loggerFactory,
        CancellationToken stopToken)
    {
        _options = options;
        _database = database;
        _channelContext = channelContext;
        _channelWriter = channelWriter;
        _workItemContext = workItemContext;
        _logger = loggerFactory.CreateLogger<RedisMessageChannelWriter>();
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
    /// ����Ϣ��ѯѭ��
    /// </summary>
    /// <returns></returns>
    private async Task PollNewMessagesLoop()
    {
        try
        {
            _logger.LogDebug("Started fetch new messages loop");
            try
            {
                // �����������飬������������Ѵ����򲶻��쳣������
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
                // ѭ����ȡ����Ϣ���������Ϣ��ַ�������ȴ�һ��ʱ��������ѯ
                var messages = await _database.StreamReadGroupAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    _channelContext.ConsumerGroup,
                    StreamPosition.NewMessages,
                    _options.PollBatchSize).ConfigureAwait(false);

                if (messages.Length > 0)
                {
                    _logger.LogDebug($"Fetched {messages.Length} new messages");
                    await DispatchMessages(messages).ConfigureAwait(false);
                }
                else
                {
                    _logger.LogDebug("No new messages, waiting for next loop");
                    await Task.Delay(_options.PollInterval, default);
                }
            }
            _logger.LogDebug("Stopped fetch new messages loop");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in fetch new messages loop");
            throw;
        }
    }

    /// <summary>
    /// ��ȡ��������Ϣѭ��
    /// ѭ�������������Ϣ�������Ϣ��ʱ�����첢�ַ�������ȴ�һ��ʱ��������ѯ
    /// </summary>
    /// <returns></returns>

    private async Task ClaimPendingMessagesLoop()
    {
        try
        {
            _logger.LogDebug("Started claim messages loop");
            while (!_stopToken.IsCancellationRequested)
            {
                await ClaimPendingMessages().ConfigureAwait(false);
                await Task.Delay(_options.PollInterval, default);
            }
            _logger.LogDebug("Stopped claim messages loop");

            async Task ClaimPendingMessages()
            {
                while (!_stopToken.IsCancellationRequested)
                {
                    var pendingMessages = await _database.StreamPendingMessagesAsync(
                        _channelContext.Topic,
                        _channelContext.ConsumerGroup,
                        _options.PollBatchSize,
                        RedisValue.Null).ConfigureAwait(false);
                    _logger.LogDebug($"Fetched {pendingMessages.Length} pending messages");

                    if (pendingMessages.Length == 0)
                    {
                        _logger.LogDebug("No messages to claim, waiting for next loop");
                        return;
                    }

                    var messagesToClaim = pendingMessages
                        .Where(msg => msg.IdleTimeInMilliseconds >= _options.ProcessingTimeout.TotalMilliseconds)
                        .Select(msg => msg.MessageId)
                        .ToArray();

                    if (messagesToClaim.Length == 0)
                    {
                        var first = pendingMessages[0];
                        _logger.LogDebug($"No timeouted messages to claim, waiting for next loop, earliest: id: {first.MessageId}, idle {first.IdleTimeInMilliseconds}ms , dc {first.DeliveryCount}");
                        return;
                    }

                    var claimedMessages = await _database.StreamClaimAsync(
                        _channelContext.Topic,
                        _channelContext.ConsumerGroup,
                        _channelContext.ConsumerGroup,
                        (long)_options.ProcessingTimeout.TotalMilliseconds,
                        messagesToClaim
                    ).ConfigureAwait(false);

                    _logger.LogDebug($"Claimed {claimedMessages.Length} messages");
                    await DispatchMessages(claimedMessages).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in claim messages loop");
            throw;
        }
    }

    /// <summary>
    /// ������Ϣ�ַ���ͨ���У���ʹ���̳߳ش������
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
            _logger.LogDebug($"Message {message.Id} dispatched to process");
        }
        _logger.LogDebug($"Dispatched {messages.Length} messages to process");
    }
}
