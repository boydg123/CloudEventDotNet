using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka ��Ϣͨ����Ϣ��ȡ��
/// </summary>
internal class KafkaMessageChannelReader
{
    private readonly Task _readLoop; // �������ж�ȡѭ��������
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

        _readLoop = Task.Run(ReadLoop, default); //����һ����̨���� _readLoop ������ ReadLoop ����
        _logger = loggerFactory.CreateLogger<KafkaMessageChannelReader>();
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
            _logger.LogDebug("Polling started");
            while (true)
            {
                // ��ͨ���ж�ȡ Kafka ��Ϣ�������������
                if (_channelReader.TryRead(out var workItem))
                {
                    //����ɹ���ȡ���������ִ�в��ȴ�����ɣ�ͬʱ����ƫ����
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
                    //���û�ж�ȡ����������ȡ�������Ƿ������������ֹͣѭ��������ȴ���һ��������
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

