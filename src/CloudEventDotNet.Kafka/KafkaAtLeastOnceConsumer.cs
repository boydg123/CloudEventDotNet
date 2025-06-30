using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 至少一次投递消费者实现。
/// 核心思想：可靠性优先。消费者在业务逻辑成功处理完消息后，才手动提交偏移量。
/// 这能确保即使在应用崩溃或处理失败时，消息也不会丢失（重启后会重新消费），但可能导致消息重复处理。
/// 
/// 实现关键：
/// 1. EnableAutoCommit = false: 禁用自动提交，改为手动控制。
/// 2. 分区通道（KafkaMessageChannel）：为每个分区维护一个独立的内存队列，保证消息按顺序处理。
/// 3. 手动提交偏移量：在通道中的消息被确认处理成功后，才在独立的提交循环（CommitLoop）中提交其偏移量。
/// 
/// 消息处理成功后手动提交偏移量，保证消息至少被处理一次。
/// 可靠性高，适用于不能丢失消息的场景。
/// </summary>
internal sealed class KafkaAtLeastOnceConsumer : ICloudEventSubscriber
{
    // Confluent.Kafka 消费者实例
    private readonly IConsumer<byte[], byte[]> _consumer;
    // 工作项上下文，包含注册中心和重发生产者
    private readonly KafkaWorkItemContext _workItemContext;
    // 要订阅的主题列表
    private readonly string[] _topics;
    // PubSub 名称
    private readonly string _pubSubName;
    // 订阅配置
    private readonly KafkaSubscribeOptions _options;
    // 日志工厂
    private readonly ILoggerFactory _loggerFactory;
    // 分区到消息通道的映射
    private readonly Dictionary<TopicPartition, KafkaMessageChannel> _channels = new();
    // 停止令牌源
    private readonly CancellationTokenSource _stopTokenSource = new();
    // 日志实例
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

        // 禁用自动提交，改为手动提交
        _options.ConsumerConfig.EnableAutoCommit = false;
        _consumer = new ConsumerBuilder<byte[], byte[]>(_options.ConsumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Consumer error: {e}", e))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogDebug("Partitions assgined: {partitions}", partitions);
                UpdateChannels(partitions);
            })
            .SetPartitionsLostHandler((c, partitions) => _logger.LogDebug("Partitions lost: {partitions}", partitions))
            .SetPartitionsRevokedHandler((c, partitions) => _logger.LogDebug("Partitions revoked: {partitions}", partitions))
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log(LogLevel.Debug, "Consumer log: {message}", log);
            })
            .Build();

        _workItemContext = new KafkaWorkItemContext(registry, new(options, loggerFactory));
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();
        _logger.LogDebug("KafkaAtLeastOnceConsumer created");

    }

    private Task _consumeLoop = default!;
    private Task _commitLoop = default!;

    /// <summary>
    /// 启动消费循环和提交循环。
    /// </summary>
    /// <returns>异步任务</returns>
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
    /// 停止消费和提交循环，并优雅关闭。
    /// </summary>
    /// <returns>异步任务</returns>
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
    /// 消费循环。
    /// 持续从 Kafka 拉取消息，并分发到对应的分区通道。
    /// </summary>
    private void ConsumeLoop()
    {
        _logger.LogDebug("Consume loop started");
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(_stopTokenSource.Token);
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    continue;
                }
                _logger.LogDebug("Fetched message {offset}", consumeResult.TopicPartitionOffset);

                // 将消息分发到对应的分区通道
                var channel = _channels[consumeResult.TopicPartition];
                channel.DispatchMessage(consumeResult);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception e)
            {
                _logger.LogError(e, "Error on consuming");
            }
        }
        _logger.LogDebug("Consume loop stopped");
    }

    /// <summary>
    /// 根据分区分配结果，更新消息通道。
    /// </summary>
    /// <param name="topicPartitions">分配到的分区</param>
    private void UpdateChannels(List<TopicPartition> topicPartitions)
    {
        // 为新分配的分区创建通道
        foreach (var topicPartition in topicPartitions)
        {
            if (!_channels.TryGetValue(topicPartition, out var _))
            {
                _channels[topicPartition] = StartChannel(topicPartition);
            }
        }

        // 停止并移除已丢失的分区通道
        var channelsToStopped = new List<KafkaMessageChannel>();
        foreach (var (tp, channel) in _channels.Where(kvp => !topicPartitions.Contains(kvp.Key)))
        {
            _channels.Remove(tp);
            channelsToStopped.Add(channel);
        }

        // 优雅停止并提交旧通道的偏移量
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
           
            return new KafkaMessageChannel(
                _options,
                channelContext,
                _workItemContext,
                _loggerFactory
            );
        }
    }

    /// <summary>
    /// 提交循环。
    /// 定期提交所有通道的偏移量。
    /// </summary>
    /// <returns>异步任务</returns>
    private async Task CommitLoop()
    {
        _logger.LogDebug("Commit loop Started");
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
    /// 提交所有通道的偏移量。
    /// </summary>
    private void CommitOffsets() => CommitOffsets(_channels.Values);

    /// <summary>
    /// 提交指定通道的偏移量。
    /// </summary>
    /// <param name="channels">要提交的通道列表</param>
    private void CommitOffsets(IEnumerable<KafkaMessageChannel> channels)
    {
        var offsetsToCommit = channels.Select(ch => ch.Reader.Offset)
                .Where(offset => offset is not null)
                .Select(offset => offset!)
                .ToList();
        if (offsetsToCommit.Any())
        {
            _logger.LogDebug("Committing offsets: {offsets}", offsetsToCommit);
            _consumer.Commit(offsetsToCommit);
        }
    }
}
