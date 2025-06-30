using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

/// <summary>
/// PubSub 构建器，负责自动扫描程序集，注册 CloudEvent 事件类型和事件处理器。
/// 采用构建器模式，支持链式调用，便于灵活配置和扩展。
/// </summary>
public class PubSubBuilder
{
    // 默认 PubSub 名称
    private readonly string _defaultPubSubName;
    // 默认 Topic
    private readonly string _defaultTopic;
    // 默认 Source
    private readonly string _defaultSource;

    /// <summary>
    /// 构造函数，初始化 PubSubBuilder 并注册核心服务。
    /// </summary>
    /// <param name="services">依赖注入服务集合</param>
    /// <param name="defaultPubSubName">默认 PubSub 名称（如 kafka、redis 等）</param>
    /// <param name="defaultTopic">默认消息主题</param>
    /// <param name="defaultSource">默认事件源</param>
    public PubSubBuilder(IServiceCollection services, string defaultPubSubName, string defaultTopic, string defaultSource)
    {
        Services = services;
        _defaultPubSubName = defaultPubSubName;
        _defaultTopic = defaultTopic;
        _defaultSource = defaultSource;

        // 注册全局选项、后台订阅服务、事件发布服务
        services.AddOptions();
        services.AddHostedService<SubscribeHostedService>();
        services.AddSingleton<ICloudEventPubSub, CloudEventPubSub>();
    }

    /// <summary>
    /// 获取服务集合，可用于后续自定义服务注册。
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// 扫描并加载指定程序集中的 CloudEvent 事件类型和事件处理器。
    /// 自动注册元数据和处理器到 Registry。
    /// </summary>
    /// <param name="assemblies">要扫描的程序集列表</param>
    /// <returns>返回自身，便于链式调用</returns>
    /// <exception cref="ArgumentException">未找到任何程序集</exception>
    /// <exception cref="InvalidOperationException">事件类型未标注 CloudEventAttribute 或处理器未正确实现</exception>
    public PubSubBuilder Load(params Assembly[] assemblies)
    {
        if (!assemblies.Any())
        {
            throw new ArgumentException("No assemblies found to scan. Supply at least one assembly to scan for handlers.");
        }

        // 创建注册中心，存储事件元数据和处理器
        var registry = new Registry(_defaultPubSubName, _defaultTopic, _defaultSource);
        var defindTypes = assemblies.SelectMany(a => a.DefinedTypes);
        foreach (var type in defindTypes)
        {
            var typeInfo = type.GetTypeInfo();
            // 跳过抽象类、接口、泛型定义等无效类型
            if (typeInfo.IsAbstract || typeInfo.IsInterface || typeInfo.IsGenericTypeDefinition || typeInfo.ContainsGenericParameters)
            {
                continue;
            }

            // 注册事件类型（带有 CloudEventAttribute 的类）
            if (type.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute)
            {
                registry.RegisterMetadata(type, attribute);
                continue;
            }

            // 注册事件处理器（实现 ICloudEventHandler<T> 的类）
            var handlerInterfaces = type
                .GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICloudEventHandler<>))
                .ToArray();
            if (!handlerInterfaces.Any()) continue;
            foreach (var handlerInterface in handlerInterfaces)
            {
                var eventDataType = handlerInterface.GenericTypeArguments[0];
                // 事件类型必须有 CloudEventAttribute
                if (eventDataType.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute2)
                {
                    registry.RegisterMetadata(eventDataType, attribute2);
                }
                else
                {
                    throw new InvalidOperationException($"Handler {type.Name} implements {handlerInterface.Name} but does not have a {nameof(CloudEventAttribute)}.");
                }
                // 注册处理器到 Registry
                typeof(Registry)
                    .GetMethod(nameof(Registry.RegisterHandler), BindingFlags.NonPublic | BindingFlags.Instance)!
                    .MakeGenericMethod(eventDataType)!
                    .Invoke(registry, new[] { (object)registry.GetMetadata(eventDataType) });
                // 注册处理器到依赖注入容器
                Services.AddScoped(handlerInterface, type);
            }
        }
        // 注册 Registry 到依赖注入容器
        Services.AddSingleton((sp) => registry.Build(sp));
        return this;
    }
}
