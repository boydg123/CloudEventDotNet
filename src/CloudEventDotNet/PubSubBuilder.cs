
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

/// <summary>
/// 配置PubSub的生成器
/// </summary>
public class PubSubBuilder
{
    private readonly string _defaultPubSubName;
    private readonly string _defaultTopic;
    private readonly string _defaultSource;

    /// <summary>
    /// PubSub 生成器
    /// </summary>
    /// <param name="services">服务收集器</param>
    /// <param name="defaultPubSubName">PubSub默认名称</param>
    /// <param name="defaultTopic">默认主题</param>
    /// <param name="defaultSource">默认源</param>
    public PubSubBuilder(IServiceCollection services, string defaultPubSubName, string defaultTopic, string defaultSource)
    {
        Services = services;
        _defaultPubSubName = defaultPubSubName;
        _defaultTopic = defaultTopic;
        _defaultSource = defaultSource;

        services.AddOptions();
        services.AddHostedService<SubscribeHostedService>();
        services.AddSingleton<ICloudEventPubSub, CloudEventPubSub>();
    }

    /// <summary>
    /// 获取一个 <see cref="IServiceProvider"/>，可用于从依赖注入容器解析服务。
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// 从指定的程序集加载 cloudevents 元数据。
    /// </summary>
    /// <param name="assemblies">程序集列表</param>
    /// <returns>PubSub生成器</returns>
    /// <exception cref="ArgumentException">未找到任何程序集</exception>
    /// <exception cref="InvalidOperationException">cloudevents 元数据格式错误</exception>
    public PubSubBuilder Load(params Assembly[] assemblies)
    {
        if (!assemblies.Any())
        {
            throw new ArgumentException("No assemblies found to scan. Supply at least one assembly to scan for handlers.");
        }

        var registry = new Registry(_defaultPubSubName, _defaultTopic, _defaultSource);
        var defindTypes = assemblies.SelectMany(a => a.DefinedTypes);
        foreach (var type in defindTypes)
        {
            var typeInfo = type.GetTypeInfo();
            if (typeInfo.IsAbstract || typeInfo.IsInterface || typeInfo.IsGenericTypeDefinition || typeInfo.ContainsGenericParameters)
            {
                continue;
            }

            if (type.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute)
            {
                registry.RegisterMetadata(type, attribute);
                continue;
            }

            var handlerInterfaces = type
                .GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICloudEventHandler<>))
                .ToArray();
            if (!handlerInterfaces.Any()) continue;
            foreach (var handlerInterface in handlerInterfaces)
            {
                var eventDataType = handlerInterface.GenericTypeArguments[0];
                if (eventDataType.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute2)
                {
                    registry.RegisterMetadata(eventDataType, attribute2);
                }
                else
                {
                    throw new InvalidOperationException($"Handler {type.Name} implements {handlerInterface.Name} but does not have a {nameof(CloudEventAttribute)}.");
                }
                typeof(Registry)
                    .GetMethod(nameof(Registry.RegisterHandler), BindingFlags.NonPublic | BindingFlags.Instance)!
                    .MakeGenericMethod(eventDataType)!
                    .Invoke(registry, new[] { (object)registry.GetMetadata(eventDataType) });
                Services.AddScoped(handlerInterface, type);
            }
        }
        Services.AddSingleton((sp) => registry.Build(sp));
        //registry.Debug();
        return this;
    }
}
