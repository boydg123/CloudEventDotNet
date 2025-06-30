using CloudEventDotNet.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

/// <summary>
/// Redis 发布订阅扩展方法。
/// 提供 AddRedisPubSub 扩展用于将 Redis PubSub 集成到 CloudEventDotNet。
/// </summary>
public static class RedisPubSubBuilderExtensions
{
    /// <summary>
    /// 向 PubSubBuilder 添加 Redis 发布/订阅能力。
    /// </summary>
    /// <param name="builder">PubSub 构建器</param>
    /// <param name="name">PubSub 名称</param>
    /// <param name="configurePublish">发布端配置委托</param>
    /// <param name="configureSubscribe">订阅端配置委托</param>
    /// <returns>支持 Redis 的 PubSubBuilder</returns>
    public static PubSubBuilder AddRedisPubSub(
        this PubSubBuilder builder,
        string name,
        Action<RedisPublishOptions>? configurePublish,
        Action<RedisSubscribeOptions>? configureSubscribe)
    {
        var services = builder.Services;

        // 配置发布端
        if (configurePublish is not null)
        {
            services.Configure<RedisPublishOptions>(name, configurePublish);
            services.Configure<PubSubOptions>(options =>
            {
                // 注册 Redis 发布者工厂
                ICloudEventPublisher factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<RedisPublishOptions>>();
                    var options = optionsFactory.Create(name);
                    return ActivatorUtilities.CreateInstance<RedisCloudEventPublisher>(sp, options);
                }
                options.PublisherFactoris[name] = factory;
            });
        }

        // 配置订阅端
        if (configureSubscribe is not null)
        {
            services.Configure<RedisSubscribeOptions>(name, configureSubscribe);
            services.Configure<PubSubOptions>(options =>
            {
                // 注册 Redis 订阅者工厂
                ICloudEventSubscriber factory(IServiceProvider sp)
                {
                    return ActivatorUtilities.CreateInstance<RedisCloudEventSubscriber>(sp, name);
                }
                options.SubscriberFactoris[name] = factory;
            });
            // 注册 Redis 消息通道工厂（用于多 Topic 消费）
            services.AddSingleton<RedisMessageChannelFactory>();
        }

        return builder;
    }
}
