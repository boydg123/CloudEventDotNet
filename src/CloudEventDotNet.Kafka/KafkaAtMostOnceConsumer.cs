using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 最多一次消费者
/// </summary>
internal sealed class KafkaAtMostOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<byte[], byte[]> _consumer; // Kafka 消费者
    private readonly KafkaWorkItemContext _workItemContext; // 消息处理上下文
    private readonly string[] _topics; // 订阅主题
    private readonly KafkaMessageChannel _channel; // 消息通道
    private readonly KafkaConsumerTelemetry _telemetry; // 消费者性能跟踪
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
            .SetErrorHandler((_, e) => _telemetry.OnConsumerError(e)) // 错误处理
            .SetPartitionsAssignedHandler((c, partitions) => _telemetry.OnPartitionsAssigned(partitions)) // 分区分配
            .SetPartitionsLostHandler((c, partitions) => _telemetry.OnPartitionsLost(partitions)) // 分区丢失
            .SetPartitionsRevokedHandler((c, partitions) => _telemetry.OnPartitionsRevoked(partitions)) // 分区撤销
            .SetLogHandler((_, log) => _telemetry.OnConsumerLog(log)) // 日志处理
            .SetOffsetsCommittedHandler((_, offsets) => _telemetry.OnConsumerOffsetsCommited(offsets)) // 偏移提交处理
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
    /// 启动消费者
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
    /// 停止消费者
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
    /// 消费循环
    /// </summary>
    private void ConsumeLoop()
    {
        _logger.LogDebug("Consume loop started");
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(_stopTokenSource.Token);
                if (consumeResult == null)
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
