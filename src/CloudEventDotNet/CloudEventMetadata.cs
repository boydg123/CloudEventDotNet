namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 元数据结构。
/// 用于描述事件的 PubSub 名称、主题、类型、源等关键信息。
/// </summary>
public sealed class CloudEventMetadata
{
    /// <summary>
    /// 事件目标 PubSub 名称（如 kafka、redis 等）。
    /// </summary>
    public string PubSubName { get; }
    /// <summary>
    /// 事件目标主题。
    /// </summary>
    public string Topic { get; }
    /// <summary>
    /// 事件类型。
    /// </summary>
    public string Type { get; }
    /// <summary>
    /// 事件源。
    /// </summary>
    public string Source { get; }

    /// <summary>
    /// 构造函数，初始化所有元数据字段。
    /// </summary>
    public CloudEventMetadata(string PubSubName, string Topic, string Type, string Source)
    {
        this.PubSubName = PubSubName;
        this.Topic = Topic;
        this.Type = Type;
        this.Source = Source;
    }
}
