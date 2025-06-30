namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 标记特性。
/// 用于标注事件数据类型，指定事件的类型、源、目标PubSub和主题等元数据信息。
/// 这些元数据用于事件的自动注册、路由和分发。
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class CloudEventAttribute : Attribute
{
    /// <summary>
    /// CloudEvents type 属性。
    /// 事件类型，用于标识事件的业务含义，常用于路由、监控、策略等场景。
    /// </summary>
    public string? Type { get; init; }

    /// <summary>
    /// CloudEvents source 属性。
    /// 事件源，描述事件的产生者。通常包含服务名、业务域、唯一标识等。
    /// </summary>
    /// <remarks>
    /// 如果未指定，使用 <see cref="PubSubBuilder"/> 配置的 defaultSource 作为默认值。
    /// </remarks>
    public string? Source { get; init; }

    /// <summary>
    /// 事件将发送到的 PubSub 名称。
    /// 用于指定消息中间件（如 kafka、redis 等）。
    /// </summary>
    /// <remarks>
    /// 如果未指定，使用 <see cref="PubSubBuilder"/> 配置的 defaultPubSubName 作为默认值。
    /// </remarks>
    public string? PubSubName { get; init; }

    /// <summary>
    /// 事件将发送到的主题。
    /// 用于指定消息的逻辑分组。
    /// </summary>
    /// <remarks>
    /// 如果未指定，使用 <see cref="PubSubBuilder"/> 配置的 defaultTopic 作为默认值。
    /// </remarks>
    public string? Topic { get; init; }
}
