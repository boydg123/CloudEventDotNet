using CloudEventDotNet.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

/// <summary>
/// PubSubBuilder 的 Kafka 扩展方法。
/// 提供了 AddKafkaPubSub 方法，用于向 CloudEventDotNet 框架注册和配置 Kafka 发布/订阅服务。
/// </summary>
public static class KafkaPubSubBuilderExtensions
{
    /// <summary>
    /// 添加一个 Kafka 发布/订阅服务。
    /// </summary>
    /// <param name="builder">PubSub 构建器</param>
    /// <param name="name">PubSub 名称（如"kafka-cluster1"）</param>
    /// <param name="configurePublish">发布者配置</param>
    /// <param name="configureSubscribe">订阅者配置</param>
    /// <returns>返回 PubSub 构建器，便于链式调用</returns>
    public static PubSubBuilder AddKafkaPubSub(
        this PubSubBuilder builder,
        string name,
        Action<KafkaPublishOptions>? configurePublish,
        Action<KafkaSubscribeOptions>? configureSubscribe)
    {
        var services = builder.Services;

        // 配置发布者
        if (configurePublish is not null)
        {
            services.Configure<KafkaPublishOptions>(name, configurePublish);
            services.Configure<PubSubOptions>(options =>
            {
                // 定义发布者工厂方法
                ICloudEventPublisher factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaPublishOptions>>();
                    var options = optionsFactory.Create(name);
                    return ActivatorUtilities.CreateInstance<KafkaCloudEventPublisher>(sp, name, options);
                }
                // 注册发布者工厂
                options.PublisherFactoris[name] = factory;
            });
        }

        // 配置订阅者
        if (configureSubscribe is not null)
        {
            services.Configure<KafkaSubscribeOptions>(name, configureSubscribe);
            services.Configure<PubSubOptions>(options =>
            {
                // 定义订阅者工厂方法
                ICloudEventSubscriber factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaSubscribeOptions>>();
                    var options = optionsFactory.Create(name);
                    // 根据投递保证选择不同的消费者实现
                    return options.DeliveryGuarantee switch
                    {
                        DeliveryGuarantee.AtMostOnce
                            => ActivatorUtilities.CreateInstance<KafkaAtMostOnceConsumer>(sp, name, options),
                        DeliveryGuarantee.AtLeastOnce
                            => ActivatorUtilities.CreateInstance<KafkaAtLeastOnceConsumer>(sp, name, options),
                        _ => throw new NotImplementedException(),
                    };
                }
                // 注册订阅者工厂
                options.SubscriberFactoris[name] = factory;
            });
        }

        return builder;
    }
}
