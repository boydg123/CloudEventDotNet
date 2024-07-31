using System.Text.Json;
using CloudEventDotNet.Redis.Instruments;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis��Ϣ��������
/// </summary>
internal sealed class RedisMessageWorkItem : IThreadPoolWorkItem
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly WorkItemWaiter _waiter = new(); // ���ڵȴ����������
    private readonly RedisWorkItemContext _context; // Redis ��������ص���������Ϣ
    private readonly ILogger _logger;

    internal RedisMessageWorkItem(
        RedisMessageChannelContext channelContext,
        RedisWorkItemContext context,
        StreamEntry message)
    {
        _context = context;
        ChannelContext = channelContext;
        Message = message;
        _logger = context.LoggerFactory.CreateLogger<RedisMessageWorkItem>();
    }

    // Redis��Ϣͨ��������
    public RedisMessageChannelContext ChannelContext { get; }
    // Redis������������
    public StreamEntry Message { get; }

    public bool Started => _started == 1; // ���ڼ�鹤�����Ƿ��Ѿ���ʼִ��
    private int _started = 0;

    public void Execute()
    {
        // ��ֹ�ظ�ִ��
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _ = ExecuteAsync();
        }
        else
        {
            return;
        }
    }

    /// <summary>
    /// �����첽�ȴ����������
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
            var cloudEvent = JSON.Deserialize<CloudEvent>((byte[])Message["data"]!)!;
            var metadata = new CloudEventMetadata(ChannelContext.PubSubName, ChannelContext.Topic, cloudEvent.Type, cloudEvent.Source);
            // ���Դ�ע����л�ȡ�������
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                return;
            }
            // �������
            bool succeed = await handler.ProcessAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            if (succeed)
            {
                // �������ɹ���ȷ����Ϣ�ѱ�����
                await _context.Redis.StreamAcknowledgeAsync(
                    ChannelContext.Topic,
                    ChannelContext.ConsumerGroup,
                    Message.Id).ConfigureAwait(false);
                _logger.LogDebug($"Message {Message.Id} acknowledged");
            }
            RedisTelemetry.OnMessageProcessed(ChannelContext.ConsumerGroup, ChannelContext.ConsumerName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to process message {Message.Id}");
        }
        finally
        {
            _waiter.SetResult();
        }
    }
}
