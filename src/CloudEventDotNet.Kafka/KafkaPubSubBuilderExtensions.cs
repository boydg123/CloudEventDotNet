using CloudEventDotNet.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;
public static class KafkaPubSubBuilderExtensions
{
    /// <summary>
    /// Add a kafka pubsub
    /// </summary>
    /// <param name="builder">配置pubsub的生成器</param>
    /// <param name="name">pubsub名称</param>
    /// <param name="configurePublish">配置发布者的方法.</param>
    /// <param name="configureSubscribe">配置订阅者的方法.</param>
    /// <returns>PubSub构造器.</returns>
    public static PubSubBuilder AddKafkaPubSub(
        this PubSubBuilder builder,
        string name,
        Action<KafkaPublishOptions>? configurePublish,
        Action<KafkaSubscribeOptions>? configureSubscribe)
    {
        var services = builder.Services;

        if (configurePublish is not null)
        {
            services.Configure<KafkaPublishOptions>(name, configurePublish);
            services.Configure<PubSubOptions>(options =>
            {
                ICloudEventPublisher factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaPublishOptions>>();
                    var options = optionsFactory.Create(name);
                    return ActivatorUtilities.CreateInstance<KafkaCloudEventPublisher>(sp, name, options);
                }
                options.PublisherFactoris[name] = factory;
            });
        }

        if (configureSubscribe is not null)
        {
            services.Configure<KafkaSubscribeOptions>(name, configureSubscribe);
            services.Configure<PubSubOptions>(options =>
            {
                ICloudEventSubscriber factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaSubscribeOptions>>();
                    var options = optionsFactory.Create(name);
                    return options.DeliveryGuarantee switch
                    {
                        DeliveryGuarantee.AtMostOnce
                            => ActivatorUtilities.CreateInstance<KafkaAtMostOnceConsumer>(sp, name, options),
                        DeliveryGuarantee.AtLeastOnce
                            => ActivatorUtilities.CreateInstance<KafkaAtLeastOnceConsumer>(sp, name, options),
                        _ => throw new NotImplementedException(),
                    };
                }
                options.SubscriberFactoris[name] = factory;
            });
        }

        return builder;
    }
}
