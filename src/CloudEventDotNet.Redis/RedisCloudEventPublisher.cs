using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis CloudEvent 发布者实现。
/// 实现 ICloudEventPublisher，负责将事件写入 Redis Stream。
/// </summary>
internal sealed class RedisCloudEventPublisher : ICloudEventPublisher
{
    /// <summary>
    /// 日志记录器。
    /// </summary>
    private readonly ILogger _logger;
    /// <summary>
    /// 发布配置。
    /// </summary>
    private readonly RedisPublishOptions _options;
    /// <summary>
    /// Redis 连接多路复用器。
    /// </summary>
    private readonly IConnectionMultiplexer _multiplexer;
    /// <summary>
    /// Redis 数据库实例。
    /// </summary>
    private readonly IDatabase _database;

    /// <summary>
    /// 构造函数，初始化发布者。
    /// </summary>
    public RedisCloudEventPublisher(ILoggerFactory loggerFactory, RedisPublishOptions options)
    {
        _options = options;
        _multiplexer = _options.ConnectionMultiplexerFactory();
        _database = _multiplexer.GetDatabase(_options.Database);
        _logger = loggerFactory.CreateLogger<RedisCloudEventPublisher>();
    }

    /// <summary>
    /// 发布 CloudEvent 到指定 Redis Stream。
    /// </summary>
    /// <typeparam name="TData">事件数据类型</typeparam>
    /// <param name="topic">Stream 名称</param>
    /// <param name="cloudEvent">事件对象</param>
    public async Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent)
    {
        byte[] data = JSON.SerializeToUtf8Bytes(cloudEvent);
        var id = await _database.StreamAddAsync(
            topic,
            "data",
            data,
            maxLength: _options.MaxLength,
            useApproximateMaxLength: true).ConfigureAwait(false);

        _logger.LogDebug($"Produced message {id.ToString()} to {topic}");
        _logger.LogDebug($"messaging.redis.client_name:{_multiplexer.ClientName}");
    }
}
