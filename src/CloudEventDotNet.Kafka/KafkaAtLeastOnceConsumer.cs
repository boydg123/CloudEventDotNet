using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka ����һ����ʵ��
/// </summary>
internal sealed class KafkaAtLeastOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<byte[], byte[]> _consumer; // Kafka ������
    private readonly KafkaWorkItemContext _workItemContext; // ��Ϣ����������
    private readonly string[] _topics; // ���ĵ�����
    private readonly KafkaConsumerTelemetry _telemetry;
    private readonly string _pubSubName; // ������������
    private readonly KafkaSubscribeOptions _options; // ����ѡ��
    private readonly ILoggerFactory _loggerFactory;
    private readonly Dictionary<TopicPartition, KafkaMessageChannel> _channels = new(); // ���������Ϣͨ��
    private readonly CancellationTokenSource _stopTokenSource = new(); // ȡ������Դ
    private readonly ILogger _logger;
    public KafkaAtLeastOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        ILoggerFactory loggerFactory
        )
    {
        _pubSubName = pubSubName;
        _options = options;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KafkaAtLeastOnceConsumer>();
        _telemetry = new KafkaConsumerTelemetry(pubSubName, loggerFactory);

        _options.ConsumerConfig.EnableAutoCommit = false;
        _consumer = new ConsumerBuilder<byte[], byte[]>(_options.ConsumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Consumer error: {e}", e)) // ������
            .SetPartitionsAssignedHandler((c, partitions) =>  // �������䴦��
            {
                _logger.LogDebug("Partitions assgined: {partitions}", partitions);
                UpdateChannels(partitions);
            })
            .SetPartitionsLostHandler((c, partitions) => _logger.LogDebug("Partitions lost: {partitions}", partitions)) // ������ʧ����
            .SetPartitionsRevokedHandler((c, partitions) => _logger.LogDebug("Partitions revoked: {partitions}", partitions)) // ������������
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log(LogLevel.Debug, "Consumer log: {message}", log);
            }) // ��־����
            .Build(); // ����������

        _workItemContext = new KafkaWorkItemContext(registry, new(options, loggerFactory));
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();
        _logger.LogDebug("KafkaAtLeastOnceConsumer created");

    }

    private Task _consumeLoop = default!;
    private Task _commitLoop = default!;

    /// <summary>
    /// ��������ѭ�����ύѭ��
    /// </summary>
    /// <returns></returns>
    public Task StartAsync()
    {
        if (_topics.Any())
        {
            _consumer.Subscribe(_topics);
            _consumeLoop = Task.Factory.StartNew(ConsumeLoop, TaskCreationOptions.LongRunning);
            _commitLoop = Task.Run(CommitLoop);
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// ȡ������ѭ�����ύѭ�������ر������ߡ�
    /// </summary>
    /// <returns></returns>
    public async Task StopAsync()
    {
        if (_topics.Any())
        {
            _stopTokenSource.Cancel();
            await _consumeLoop;
            await _commitLoop;
            await Task.WhenAll(_channels.Values.Select(ch => ch.StopAsync()));
            CommitOffsets();
            _consumer.Unsubscribe();
        }
        _consumer.Close();
    }

    /// <summary>
    /// ����ѭ��
    /// </summary>
    private void ConsumeLoop()
    {
        _logger.LogDebug("Consume loop started");
        // ����������Ϣ��ֱ��ȡ�����Ʊ�����
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(_stopTokenSource.Token);
                //����Ƿ񵽴��˷�����ĩβ����������˷�����ĩβ��˵���÷�����ǰû�и������Ϣ��ͬ��������������������һ��ѭ��
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    continue;
                }
                _logger.LogDebug("Fetched message {offset}", consumeResult.TopicPartitionOffset);

                // ������Ϣ�󣬽���Ϣ���ɵ���Ӧ��ͨ��
                var channel = _channels[consumeResult.TopicPartition];
                channel.DispatchMessage(consumeResult);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception e)
            {
                _logger.LogError(e, "Error on consuming");
            }
        }
        _telemetry.OnConsumeLoopStopped();
        _logger.LogDebug("Consume loop stopped");
    }

    /// <summary>
    /// ���ݷ���ķ�������ͨ���ֵ�
    /// </summary>
    /// <param name="topicPartitions"></param>
    private void UpdateChannels(List<TopicPartition> topicPartitions)
    {
        foreach (var topicPartition in topicPartitions)
        {
            if (!_channels.TryGetValue(topicPartition, out var _))
            {
                _channels[topicPartition] = StartChannel(topicPartition);
            }
        }

        var channelsToStopped = new List<KafkaMessageChannel>();
        foreach (var (tp, channel) in _channels.Where(kvp => !topicPartitions.Contains(kvp.Key)))
        {
            _channels.Remove(tp);
            channelsToStopped.Add(channel);
        }

        // ����������ٷ��䣬��ֹͣ��Ӧ��ͨ�����ύƫ����
        // wait all pending messages checked
        Task.WhenAll(channelsToStopped.Select(ch => ch.StopAsync())).GetAwaiter().GetResult();
        CommitOffsets(channelsToStopped);

        KafkaMessageChannel StartChannel(TopicPartition tp)
        {
            var channelContext = new KafkaMessageChannelContext(
                _pubSubName,
                _consumer.Name,
                _options.ConsumerConfig.GroupId,
                tp
            );
            var telemetry = new KafkaMessageChannelTelemetry(
                _loggerFactory,
                channelContext
            );
            return new KafkaMessageChannel(
                _options,
                channelContext,
                _workItemContext,
                _loggerFactory
            );
        }
    }

    /// <summary>
    /// �����ύƫ������ֱ��ȡ�����Ʊ�����
    /// </summary>
    /// <returns></returns>
    private async Task CommitLoop()
    {
        _telemetry.OnCommitLoopStarted();
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                CommitOffsets();
                await Task.Delay(TimeSpan.FromSeconds(10), _stopTokenSource.Token).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in commit loop");
            }
        }
        _logger.LogDebug("Commit loop stopped");
    }

    /// <summary>
    /// �ύͨ���е�ƫ����
    /// </summary>
    private void CommitOffsets() => CommitOffsets(_channels.Values);

    /// <summary>
    /// �ύͨ���е�ƫ����
    /// </summary>
    /// <param name="channels"></param>
    private void CommitOffsets(IEnumerable<KafkaMessageChannel> channels)
    {
        try
        {
            var offsets = channels
                .Where(ch => ch.Reader.Offset != null)
                .Select(ch => ch.Reader.Offset)
                .Select(offset => new TopicPartitionOffset(offset!.TopicPartition, offset.Offset + 1))
                .ToArray();
            _consumer.Commit(offsets);
            _logger.LogDebug("Committed offsets: {offsets}", offsets);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error on commit offsets");
            throw;
        }
    }
}
