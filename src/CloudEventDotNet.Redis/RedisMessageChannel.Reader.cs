using System.Threading.Channels;

namespace CloudEventDotNet.Redis;
/// <summary>
/// Redis��Ϣͨ����ȡ��
/// </summary>
internal sealed partial class RedisMessageChannelReader
{
    // �ô��붨����һ����Ϊ RedisMessageChannelReader ���࣬��Ҫ���ڴ�ͨ���ж�ȡ RedisMessageWorkItem ���󲢴������ǡ�����Ҫ���ܰ�����
    // ��ͨ���ж�ȡ�����ִ�С�
    // ��¼ң����Ϣ�Լ�غ͵��ԡ�
    // ֧��ȡ���������ܹ����յ�ȡ������ʱ���ŵ�ֹͣ��
    // �����쳣�����ȷ��ϵͳ���ȶ��ԡ�
    private readonly ChannelReader<RedisMessageWorkItem> _channelReader; // ���ڴ�ͨ���ж�ȡ RedisMessageWorkItem ����
    private readonly CancellationToken _stopToken;
    private readonly RedisMessageTelemetry _telemetry;
    private readonly Task _readLoop; // ����ִ�ж�ȡѭ��������

    public RedisMessageChannelReader(
        ChannelReader<RedisMessageWorkItem> channelReader,
        RedisMessageTelemetry telemetry,
        CancellationToken stopToken)
    {
        _channelReader = channelReader;
        _stopToken = stopToken;
        _telemetry = telemetry;
        _readLoop = Task.Run(ReadLoop, default);
    }

    public Task StopAsync()
    {
        return _readLoop;
    }

    // ���ڵȴ���ȡѭ�������
    private async Task ReadLoop()
    {
        try
        {
            _telemetry.OnMessageChannelReaderStarted();
            while (true) // ����һ������ѭ�������Դ�ͨ���ж�ȡ������
            {
                // ����ɹ���ȡ�������������Ƿ������������δ������ִ�в���¼ң����Ϣ
                if (_channelReader.TryRead(out var workItem))
                {
                    if (!workItem.Started)
                    {
                        _telemetry.OnWorkItemStarting();
                        workItem.Execute();
                        _telemetry.OnWorkItemStarted();
                    }
                    //�ȴ���������ɣ����δ������¼ң����Ϣ���ȴ������
                    var vt = workItem.WaitToCompleteAsync();
                    if (!vt.IsCompletedSuccessfully)
                    {
                        _telemetry.OnWaitingWorkItemComplete();
                        await vt.ConfigureAwait(false);
                        _telemetry.OnWorkItemCompleted();
                    }
                }
                else
                {
                    // ���δ��ȡ����������ȡ�������Ƿ�������ȡ������������¼ң����Ϣ���˳�ѭ����
                    if (_stopToken.IsCancellationRequested)
                    {
                        _telemetry.MessageChannelReaderStopped();
                        return;
                    }
                    else
                    {
                        // ���û��ȡ�����󣬼�¼�ȴ���һ���������ң����Ϣ���ȴ�ͨ���ɶ�
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
