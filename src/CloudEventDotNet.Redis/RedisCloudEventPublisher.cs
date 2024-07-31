using System.Text.Json;
using CloudEventDotNet.Redis.Instruments;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis CloudEvent ������
/// </summary>
internal sealed class RedisCloudEventPublisher : ICloudEventPublisher
{
    private readonly ILogger _logger;
    private readonly RedisPublishOptions _options;
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IDatabase _database;

    public RedisCloudEventPublisher(ILoggerFactory loggerFactory, RedisPublishOptions options)
    {
        _options = options;
        _multiplexer = _options.ConnectionMultiplexerFactory();
        _database = _multiplexer.GetDatabase(_options.Database);
        _logger = loggerFactory.CreateLogger<RedisCloudEventPublisher>();
    }

    /// <summary>
    /// ������Ϣ
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="topic"></param>
    /// <param name="cloudEvent"></param>
    /// <returns></returns>
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
