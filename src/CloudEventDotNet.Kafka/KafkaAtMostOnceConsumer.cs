using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka ���һ��������
/// </summary>
internal sealed class KafkaAtMostOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<byte[], byte[]> _consumer; // Kafka ������
    private readonly KafkaWorkItemContext _workItemContext; // ��Ϣ����������
    private readonly string[] _topics; // ��������
    private readonly KafkaMessageChannel _channel; // ��Ϣͨ��
    private readonly KafkaConsumerTelemetry _telemetry; // ���������ܸ���
    private readonly CancellationTokenSource _stopTokenSource = new();
    private readonly ILogger<KafkaRedeliverProducer> _logger;

    public KafkaAtMostOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        ILoggerFactory loggerFactory,
        ILogger<KafkaRedeliverProducer> logger)
    {
        _telemetry = new KafkaConsumerTelemetry(pubSubName, loggerFactory);
        _logger = logger;
        _consumer = new ConsumerBuilder<byte[], byte[]>(options.ConsumerConfig)
            .SetErrorHandler((_, e) => _telemetry.OnConsumerError(e)) // ������
            .SetPartitionsAssignedHandler((c, partitions) => _telemetry.OnPartitionsAssigned(partitions)) // ��������
            .SetPartitionsLostHandler((c, partitions) => _telemetry.OnPartitionsLost(partitions)) // ������ʧ
            .SetPartitionsRevokedHandler((c, partitions) => _telemetry.OnPartitionsRevoked(partitions)) // ��������
            .SetLogHandler((_, log) => _telemetry.OnConsumerLog(log)) // ��־����
            .SetOffsetsCommittedHandler((_, offsets) => _telemetry.OnConsumerOffsetsCommited(offsets)) // ƫ���ύ����
        .Build();

        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };
        _workItemContext = new KafkaWorkItemContext(registry, new(options, loggerFactory));
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();

        var channelContext = new KafkaMessageChannelContext(
            pubSubName,
            _consumer.Name,
            options.ConsumerConfig.GroupId,
            new TopicPartition("*", -1)
        );
        var telemetry = new KafkaMessageChannelTelemetry(
            loggerFactory,
            channelContext
        );
        _channel = new KafkaMessageChannel(
            options,
            channelContext,
            _workItemContext,
            loggerFactory
        );
    }

    private Task _consumeLoop = default!;
    /// <summary>
    /// ����������
    /// </summary>
    /// <returns></returns>
    public Task StartAsync()
    {
        if (_topics.Any())
        {
            _consumer.Subscribe(_topics);
            _consumeLoop = Task.Factory.StartNew(ConsumeLoop, TaskCreationOptions.LongRunning);
        }
        return Task.CompletedTask;
    }
    /// <summary>
    /// ֹͣ������
    /// </summary>
    /// <returns></returns>
    public async Task StopAsync()
    {
        if (_topics.Any())
        {
            _consumer.Unsubscribe();
            _stopTokenSource.Cancel();
            await _consumeLoop;
            _consumer.Close();
            await _channel.StopAsync();
        }
        else
        {
            _consumer.Close();
        }
    }

    /// <summary>
    /// ����ѭ��
    /// </summary>
    private void ConsumeLoop()
    {
        _logger.LogDebug("Consume loop started");
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
                _channel.DispatchMessage(consumeResult);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception e)
            {
                _logger.LogError(e, "Error on consuming");
            }
        }
        _logger.LogDebug("Consume loop stopped");
    }
}
