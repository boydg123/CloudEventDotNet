using System.Collections.Concurrent;
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
    private readonly CancellationTokenSource _stopTokenSource = new();
    private readonly ILogger<KafkaRedeliverProducer> _logger;

    public KafkaAtMostOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        ILoggerFactory loggerFactory,
        ILogger<KafkaRedeliverProducer> logger)
    {
        _logger = logger;
        _consumer = new ConsumerBuilder<byte[], byte[]>(options.ConsumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Consumer error: {e}", e)) // 错误处理
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogDebug("Partitions assgined: {partitions}", partitions);
            }) // 分区分配
            .SetPartitionsLostHandler((c, partitions) => _logger.LogDebug("Partitions lost: {partitions}", partitions)) // 分区丢失
            .SetPartitionsRevokedHandler((c, partitions) => _logger.LogDebug("Partitions revoked: {partitions}", partitions)) // 分区撤销
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log(LogLevel.Debug, "Consumer log: {message}", log);
            }) // 日志处理
            .SetOffsetsCommittedHandler((_, offsets) => _logger.LogDebug($"Consumer Offsets Commited：{offsets}")) // 偏移提交处理
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

                //检查是否到达了分区的末尾。如果到达了分区的末尾，说明该分区当前没有更多的消息，同样跳过后续处理，继续下一次循环
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
