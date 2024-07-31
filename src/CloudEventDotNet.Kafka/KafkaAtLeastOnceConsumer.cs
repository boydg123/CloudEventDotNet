using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 最少一次消实现
/// </summary>
internal sealed class KafkaAtLeastOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<byte[], byte[]> _consumer; // Kafka 消费者
    private readonly KafkaWorkItemContext _workItemContext; // 消息处理上下文
    private readonly string[] _topics; // 订阅的主题
    private readonly KafkaConsumerTelemetry _telemetry;
    private readonly string _pubSubName; // 发布订阅名称
    private readonly KafkaSubscribeOptions _options; // 订阅选项
    private readonly ILoggerFactory _loggerFactory;
    private readonly Dictionary<TopicPartition, KafkaMessageChannel> _channels = new(); // 主题分区消息通道
    private readonly CancellationTokenSource _stopTokenSource = new(); // 取消令牌源
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
            .SetErrorHandler((_, e) => _logger.LogError("Consumer error: {e}", e)) // 错误处理
            .SetPartitionsAssignedHandler((c, partitions) =>  // 分区分配处理
            {
                _logger.LogDebug("Partitions assgined: {partitions}", partitions);
                UpdateChannels(partitions);
            })
            .SetPartitionsLostHandler((c, partitions) => _logger.LogDebug("Partitions lost: {partitions}", partitions)) // 分区丢失处理
            .SetPartitionsRevokedHandler((c, partitions) => _logger.LogDebug("Partitions revoked: {partitions}", partitions)) // 分区撤销处理
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log(LogLevel.Debug, "Consumer log: {message}", log);
            }) // 日志处理
            .Build(); // 创建消费者

        _workItemContext = new KafkaWorkItemContext(registry, new(options, loggerFactory));
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();
        _logger.LogDebug("KafkaAtLeastOnceConsumer created");

    }

    private Task _consumeLoop = default!;
    private Task _commitLoop = default!;

    /// <summary>
    /// 启动消费循环和提交循环
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
    /// 取消消费循环和提交循环，并关闭消费者。
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
    /// 消费循环
    /// </summary>
    private void ConsumeLoop()
    {
        _logger.LogDebug("Consume loop started");
        // 持续消费消息，直到取消令牌被请求
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

                // 消费消息后，将消息分派到相应的通道
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
    /// 根据分配的分区更新通道字典
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

        // 如果分区不再分配，则停止相应的通道并提交偏移量
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
    /// 定期提交偏移量，直到取消令牌被请求
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
    /// 提交通道中的偏移量
    /// </summary>
    private void CommitOffsets() => CommitOffsets(_channels.Values);

    /// <summary>
    /// 提交通道中的偏移量
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
