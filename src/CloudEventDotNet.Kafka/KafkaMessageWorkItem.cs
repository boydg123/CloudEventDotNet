using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka��Ϣ��������
/// </summary>
internal sealed class KafkaMessageWorkItem : IThreadPoolWorkItem
{
    private readonly KafkaMessageChannelContext _channelContext; // ��Ϣͨ��
    private readonly KafkaWorkItemContext _context; // 
    private readonly KafkaMessageChannelTelemetry _telemetry;
    private readonly ConsumeResult<byte[], byte[]> _message; // ��Ϣ
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    internal KafkaMessageWorkItem(
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext context,
        KafkaMessageChannelTelemetry telemetry,
        ConsumeResult<byte[], byte[]> message)
    {
        _channelContext = channelContext;
        _context = context;
        _telemetry = telemetry;
        _message = message;
    }

    // ����Ϣ�������������ƫ����
    public TopicPartitionOffset TopicPartitionOffset => _message.TopicPartitionOffset;

    public bool Started => _started == 1; // ���ڼ����Ϣ�����Ƿ��Ѿ���ʼ
    private int _started = 0;

    public void Execute()
    {
        // ȷ��ExecuteAsyncֻ������һ��
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _ = ExecuteAsync();
        }
        else
        {
            return;
        }
    }

    private readonly WorkItemWaiter _waiter = new();
    /// <summary>
    /// ���ڵȴ��첽�������
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
            var cloudEvent = JSON.Deserialize<CloudEvent>(_message.Message.Value)!;
            var metadata = new CloudEventMetadata(_channelContext.PubSubName, _message.Topic, cloudEvent.Type, cloudEvent.Source);
            // ���Դ�ע����л�ȡ�������
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                CloudEventProcessingTelemetry.OnHandlerNotFound(_telemetry.Logger, metadata);
                return;
            }
            // ����ҵ���������첽������Ϣ��
            bool succeed = await handler.ProcessAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            KafkaConsumerTelemetry.OnConsumed(_channelContext.ConsumerName, _channelContext.ConsumerGroup);

            if (!succeed)
            {
                // �������ʧ�ܣ����·�����Ϣ
                await _context.Producer.ReproduceAsync(_message).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _telemetry.Logger.LogError(ex, "Error handling Kafka message");
        }
        finally
        {
            _waiter.SetResult();
        }
    }
}

