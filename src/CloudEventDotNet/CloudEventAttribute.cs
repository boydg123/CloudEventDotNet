namespace CloudEventDotNet;

/// <summary>
/// CloudEvent标记
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class CloudEventAttribute : Attribute
{
    /// <summary>
    /// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#type">'type'</see> attribute.
    /// Type of occurrence which has happened.
    /// Often this attribute is used for routing, observability, policy enforcement, etc.
    /// CloudEvent类型，可用于路由，可观察性，策略强制等。
    /// </summary>
    public string? Type { get; init; }

    /// <summary>
    /// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#source">'source'</see> attribute.
    /// CloudEvent源，描述事件的产生者。通常包含事件源的类型，组织发布事件，事件产生的过程，以及一些唯一标识符。
    /// </summary>
    /// <remarks>
    /// 在<see cref="PubSubBuilder"/>配置defaultSource作为默认源.
    /// </remarks>
    public string? Source { get; init; }

    /// <summary>
    /// CloudEvents将发送到的PubSub的名称
    /// </summary>
    /// <remarks>
    /// 在<see cref="PubSubBuilder"/>中的配置defaultPubSubName 默认名称.
    /// </remarks>
    public string? PubSubName { get; init; }

    /// <summary>
    /// CloudEvents将发送到的主题
    /// </summary>
    /// <remarks>
    /// 在 <see cref="PubSubBuilder"/>的配置中使用defaultTopic 配置默认主题.
    /// </remarks>
    public string? Topic { get; init; }
}
