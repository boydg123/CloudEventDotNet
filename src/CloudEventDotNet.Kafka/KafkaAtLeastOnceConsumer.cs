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
    public KafkaAtLeastOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        ILoggerFactory loggerFactory)
    {
        _pubSubName = pubSubName;
        _options = options;
        _loggerFactory = loggerFactory;
        _telemetry = new KafkaConsumerTelemetry(pubSubName, loggerFactory);

        _options.ConsumerConfig.EnableAutoCommit = false;
        _consumer = new ConsumerBuilder<byte[], byte[]>(_options.ConsumerConfig)
            .SetErrorHandler((_, e) => _telemetry.OnConsumerError(e)) // 错误处理
            .SetPartitionsAssignedHandler((c, partitions) =>  // 分区分配处理
            {
                _telemetry.OnPartitionsAssigned(partitions);
                UpdateChannels(partitions);
            })
            .SetPartitionsLostHandler((c, partitions) => _telemetry.OnPartitionsLost(partitions)) // 分区丢失处理
            .SetPartitionsRevokedHandler((c, partitions) => _telemetry.OnPartitionsRevoked(partitions)) // 分区撤销处理
            .SetLogHandler((_, log) => _telemetry.OnConsumerLog(log)) // 日志处理
            .Build(); // 创建消费者

        _workItemContext = new KafkaWorkItemContext(registry, new(options, _telemetry));
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();
        _telemetry.Logger.LogDebug("KafkaAtLeastOnceConsumer created");
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
        _telemetry.OnConsumeLoopStarted();
        // 持续消费消息，直到取消令牌被请求
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(_stopTokenSource.Token);
                if (consumeResult == null)
                {
                    continue;
                }
                _telemetry.OnMessageFetched(consumeResult.TopicPartitionOffset);

                // 消费消息后，将消息分派到相应的通道
                var channel = _channels[consumeResult.TopicPartition];
                channel.DispatchMessage(consumeResult);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception e)
            {
                _telemetry.OnConsumeFailed(e);
            }
        }
        _telemetry.OnConsumeLoopStopped();
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
                telemetry
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
                _telemetry.OnCommitLoopError(ex);
            }
        }
        _telemetry.OnCommitLoopStopped();
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
            _telemetry.OnOffsetsCommited(offsets);
        }
        catch (Exception ex)
        {
            _telemetry.Logger.LogError(ex, "Error on commit offsets");
            throw;
        }
    }
}
