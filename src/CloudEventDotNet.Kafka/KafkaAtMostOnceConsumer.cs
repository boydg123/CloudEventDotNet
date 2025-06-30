using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

/// <summary>
/// Kafka 最多一次投递消费者实现。
/// 核心思想：性能优先，可靠性次之。消费者拉取消息后，由Kafka客户端自动、定期地提交偏移量，
/// 而不关心消息是否被业务逻辑成功处理。这换来了极高的处理吞吐量，但在应用崩溃等场景下可能丢失消息。
/// 
/// 实现关键：
/// 1. EnableAutoCommit = true: 启用自动提交偏移量。
/// 2. 无需手动管理偏移量：没有复杂的提交循环和状态跟踪。
/// 3. 无需消息通道：消息被直接放入线程池处理，不保证顺序和处理状态。
/// 
/// 消息拉取后立即提交偏移量，不保证消息一定被处理。
/// 性能较高，适用于可接受少量消息丢失的场景。
/// </summary>
internal sealed class KafkaAtMostOnceConsumer : ICloudEventSubscriber
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
    // 停止令牌源
    private readonly CancellationTokenSource _stopTokenSource = new();
    // 日志实例
    private readonly ILogger _logger;

    public KafkaAtMostOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        ILoggerFactory loggerFactory
        )
    {
        _pubSubName = pubSubName;
        _options = options;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KafkaAtMostOnceConsumer>();

        // 启用自动提交偏移量
        _options.ConsumerConfig.EnableAutoCommit = true;
        _consumer = new ConsumerBuilder<byte[], byte[]>(_options.ConsumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Consumer error: {e}", e))
            .SetLogHandler((_, log) =>
            {
                int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
                _logger.Log(LogLevel.Debug, "Consumer log: {message}", log);
            })
            .Build();

        _workItemContext = new KafkaWorkItemContext(registry, new(options, loggerFactory));
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();
        _logger.LogDebug("KafkaAtMostOnceConsumer created");

    }

    private Task _consumeLoop = default!;

    /// <summary>
    /// 启动消费循环。
    /// </summary>
    /// <returns>异步任务</returns>
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
    /// 停止消费循环并关闭消费者。
    /// </summary>
    /// <returns>异步任务</returns>
    public async Task StopAsync()
    {
        if (_topics.Any())
        {
            _stopTokenSource.Cancel();
            await _consumeLoop;
            _consumer.Unsubscribe();
        }
        _consumer.Close();
    }

    /// <summary>
    /// 消费循环。
    /// 持续从 Kafka 拉取消息并分发给工作项处理。
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

                // 创建工作项并放入线程池执行
                var workItem = new KafkaMessageWorkItem(
                    new KafkaMessageChannelContext(_pubSubName, _consumer.Name, _options.ConsumerConfig.GroupId, consumeResult.TopicPartition),
                    _workItemContext,
                    _loggerFactory,
                    consumeResult);
                ThreadPool.UnsafeQueueUserWorkItem(workItem, false);
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
