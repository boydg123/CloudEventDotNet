using System.Threading.Channels;
using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka ��Ϣͨ����Ϣ��ȡ��
/// </summary>
internal class KafkaMessageChannelReader
{
    private readonly Task _readLoop; // �������ж�ȡѭ��������
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

        _readLoop = Task.Run(ReadLoop, default); //����һ����̨���� _readLoop ������ ReadLoop ����
    }

    // ���ڴ洢��ǰ����� Kafka ��Ϣ��ƫ����
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
                // ��ͨ���ж�ȡ Kafka ��Ϣ�������������
                if (_channelReader.TryRead(out var workItem))
                {
                    //����ɹ���ȡ���������ִ�в��ȴ�����ɣ�ͬʱ����ƫ����
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
                    //���û�ж�ȡ����������ȡ�������Ƿ������������ֹͣѭ��������ȴ���һ��������
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

