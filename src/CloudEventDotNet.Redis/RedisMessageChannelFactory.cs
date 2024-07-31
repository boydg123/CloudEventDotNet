using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis��Ϣͨ������
/// </summary>
internal class RedisMessageChannelFactory
{
    // ��Ҫ���ܣ�RedisMessageChannelFactory �����Ҫ�����Ǵ����͹��� Redis ��Ϣͨ������ͨ������ע���ȡ��Ҫ�����ú͹��ߣ�Ȼ����ݸ����� pubSubName ������Ӧ�� Redis
    // ��Ϣͨ�����顣ÿ��ͨ����������ϸ����������Ϣ��ң���¼���Ա���õع���ͼ�� Redis ��Ϣ�Ķ��ĺʹ���.
    // �ؼ����裺
    // 1. ͨ������ע���ȡ IOptionsFactory<RedisSubscribeOptions>��Registry��IServiceScopeFactory �� ILoggerFactory ʵ����
    // 2. ���� pubSubName ���� RedisSubscribeOptions ʵ��������ȡ ConnectionMultiplexer �� Redis ���ݿ�ʵ����
    // 3. ��ȡ���ĵ������б���Ϊÿ�����ⴴ�� RedisMessageChannel ʵ����
    // 4. Ϊÿ��ͨ��������ϸ����������Ϣ��ң���¼�����շ�����Щͨ�������顣

    private readonly IOptionsFactory<RedisSubscribeOptions> _optionsFactory;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILoggerFactory _loggerFactory;

    public RedisMessageChannelFactory(
        IOptionsFactory<RedisSubscribeOptions> optionsFactory,
        Registry registry,
        IServiceScopeFactory scopeFactory,
        ILoggerFactory loggerFactory)
    {
        _optionsFactory = optionsFactory;
        _registry = registry;
        _scopeFactory = scopeFactory;
        _loggerFactory = loggerFactory;
    }

    /// <summary>
    /// ���� Redis ��Ϣͨ������
    /// </summary>
    /// <param name="pubSubName"></param>
    /// <returns></returns>
    public RedisMessageChannel[] Create(string pubSubName)
    {
        var options = _optionsFactory.Create(pubSubName);
        var multiplexer = options.ConnectionMultiplexerFactory();
        var redis = multiplexer.GetDatabase(options.Database);

        // ��ȡ���ĵ������б���Ϊÿ��������� Create �������� RedisMessageChannel ʵ�������շ�����Щʵ��������
        return _registry.GetSubscribedTopics(pubSubName)
            .Select(topic => Create(pubSubName, topic, options, redis)).ToArray();
    }

    /// <summary>
    /// �������� Redis ��Ϣͨ��
    /// </summary>
    /// <param name="pubSubName"></param>
    /// <param name="topic"></param>
    /// <param name="options"></param>
    /// <param name="redis"></param>
    /// <returns></returns>
    private RedisMessageChannel Create(
        string pubSubName,
        string topic,
        RedisSubscribeOptions options,
        IDatabase redis)
    {
        var logger = _loggerFactory.CreateLogger($"{nameof(RedisCloudEventSubscriber)}[{pubSubName}:{topic}]");
        var channelContext = new RedisMessageChannelContext(
            pubSubName,
            redis.Multiplexer.ClientName,
            options.ConsumerGroup,
            topic
        );
        var workItemContext = new RedisWorkItemContext(
            _registry,
            _scopeFactory,
            redis,
            _loggerFactory
        );
        var channel = new RedisMessageChannel(
            options,
            redis,
            channelContext,
            workItemContext,
            _loggerFactory);
        return channel;
    }

}
