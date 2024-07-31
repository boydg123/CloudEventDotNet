using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka��Ϣͨ��
/// </summary>
internal class KafkaMessageChannel
{
    private readonly Channel<KafkaMessageWorkItem> _channel; //���ڴ洢 Kafka ��Ϣ�������ͨ��
    private readonly CancellationTokenSource _stopTokenSource = new(); //����ֹͣ��Ϣͨ���� CancellationTokenSource
    private readonly KafkaMessageChannelContext _channelContext; //��Ϣͨ��������
    private readonly KafkaWorkItemContext _workItemContext; //������������
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;

    public KafkaMessageChannel(
        KafkaSubscribeOptions options,
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext workItemContext,
        ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KafkaMessageChannel>();
        if (options.RunningWorkItemLimit > 0)
        {
            // �����н�(������)ͨ��
            _channel = Channel.CreateBounded<KafkaMessageWorkItem>(new BoundedChannelOptions(options.RunningWorkItemLimit)
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created bounded channel");
        }
        else
        {
            // �����޽�(������)ͨ�� - ���ܻ�ľ��ڴ���Դ
            _channel = Channel.CreateUnbounded<KafkaMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created unbounded channel");
        }

        Reader = new KafkaMessageChannelReader(
            _channel.Reader,
            loggerFactory,
            _stopTokenSource.Token);
        _channelContext = channelContext;
        _workItemContext = workItemContext;
    }

    public bool IsActive { get; }

    /// <summary>
    /// ֹͣд��
    /// </summary>
    /// <returns></returns>
    public Task StopAsync()
    {
        _stopTokenSource.Cancel();
        return Reader.StopAsync();
    }

    /// <summary>
    /// д����Ϣ��Channel
    /// </summary>
    /// <param name="message"></param>
    public void DispatchMessage(ConsumeResult<byte[], byte[]> message)
    {
        var workItem = new KafkaMessageWorkItem(
            _channelContext,
            _workItemContext,
            _loggerFactory,
            message);

        if (!_channel.Writer.TryWrite(workItem))
        {
            _channel.Writer.WriteAsync(workItem).AsTask().GetAwaiter().GetResult();
        }
        //ThreadPool.UnsafeQueueUserWorkItem(workItem, false);
    }

    public KafkaMessageChannelReader Reader { get; }
}
