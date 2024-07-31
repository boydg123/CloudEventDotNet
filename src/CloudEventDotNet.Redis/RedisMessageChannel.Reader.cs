using System.Threading.Channels;
using Microsoft.Extensions.Logging;

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
    private readonly Task _readLoop; // ����ִ�ж�ȡѭ��������
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

    // ���ڵȴ���ȡѭ�������
    private async Task ReadLoop()
    {
        try
        {
            _logger.LogInformation("Polling started.");
            while (true) // ����һ������ѭ�������Դ�ͨ���ж�ȡ������
            {
                // ����ɹ���ȡ�������������Ƿ������������δ������ִ�в���¼ң����Ϣ
                if (_channelReader.TryRead(out var workItem))
                {
                    if (!workItem.Started)
                    {
                        _logger.LogTrace("Work item not started, starting it");
                        workItem.Execute();
                        _logger.LogTrace("Work item started");
                    }
                    //�ȴ���������ɣ����δ������¼ң����Ϣ���ȴ������
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
                    // ���δ��ȡ����������ȡ�������Ƿ�������ȡ������������¼ң����Ϣ���˳�ѭ����
                    if (_stopToken.IsCancellationRequested)
                    {
                        _logger.LogDebug("Reader stopped");
                        return;
                    }
                    else
                    {
                        // ���û��ȡ�����󣬼�¼�ȴ���һ���������ң����Ϣ���ȴ�ͨ���ɶ�
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
