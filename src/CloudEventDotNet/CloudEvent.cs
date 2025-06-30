using System.Text.Json;
using System.Text.Json.Serialization;

namespace CloudEventDotNet;

/// <summary>
/// 表示一个 CloudEvent 事件（原始 JSON 数据版本）。
/// </summary>
/// <remarks>
/// 该类型用于内部处理 CloudEvent 的原始 JSON 数据，便于反序列化和通用处理。
/// </remarks>
internal record CloudEvent(
    [property: JsonPropertyName("id")]
    string Id,           // 事件唯一标识符
    [property: JsonPropertyName("source")]
    string Source,       // 事件源标识
    [property: JsonPropertyName("type")]
    string Type,         // 事件类型
    [property: JsonPropertyName("time")]
    DateTimeOffset Time, // 事件发生时间
    [property: JsonPropertyName("data")]
    JsonElement Data,    // 事件数据（原始 JSON）
    [property: JsonPropertyName("dataschema")]
    Uri? DataSchema,     // 数据模式 URI，可选
    [property: JsonPropertyName("subject")]
    string? Subject      // 事件主题，可选
)
{
    /// <summary>
    /// 数据内容类型，默认为 application/json。
    /// </summary>
    [JsonPropertyName("datacontenttype")]
    public string DataContentType { get; } = "application/json";

    /// <summary>
    /// CloudEvents 规范版本，默认为 1.0。
    /// </summary>
    [JsonPropertyName("specversion")]
    public string SpecVersion { get; } = "1.0";

    /// <summary>
    /// CloudEvent 扩展属性字典，可存储自定义扩展字段。
    /// </summary>
    [JsonExtensionData]
    public Dictionary<string, JsonElement> Extensions { get; set; } = new();
}

/// <summary>
/// 表示一个强类型的 CloudEvent 事件。
/// </summary>
/// <typeparam name="TData">CloudEvent 数据类型</typeparam>
/// <param name="Id">事件唯一标识符。与 <see cref="Source"/> 组合可实现事件去重。</param>
/// <param name="Source">事件源标识。描述事件生产者。</param>
/// <param name="Type">事件类型。用于路由、监控等。</param>
/// <param name="Time">事件发生时间。</param>
/// <param name="Data">事件数据（强类型）。</param>
/// <param name="DataSchema">数据模式 URI，指向数据结构的定义。</param>
/// <param name="Subject">事件主题，描述事件在生产者上下文中的具体对象。</param>
public sealed record CloudEvent<TData>(
    [property: JsonPropertyName("id")]
    string Id,           // 事件唯一标识符
    [property: JsonPropertyName("source")]
    string Source,       // 事件源标识
    [property: JsonPropertyName("type")]
    string Type,         // 事件类型
    [property: JsonPropertyName("time")]
    DateTimeOffset Time, // 事件发生时间
    [property: JsonPropertyName("data")]
    TData Data,          // 事件数据（强类型）
    [property: JsonPropertyName("dataschema")]
    Uri? DataSchema,     // 数据模式 URI，可选
    [property: JsonPropertyName("subject")]
    string? Subject      // 事件主题，可选
)
{
    /// <summary>
    /// 数据内容类型，默认为 application/json。
    /// </summary>
    [JsonPropertyName("datacontenttype")]
    public string DataContentType { get; } = "application/json";

    /// <summary>
    /// CloudEvents 规范版本，默认为 1.0。
    /// </summary>
    [JsonPropertyName("specversion")]
    public string SpecVersion { get; } = "1.0";

    /// <summary>
    /// CloudEvent 扩展属性字典，可存储自定义扩展字段。
    /// </summary>
    [JsonExtensionData]
    public Dictionary<string, object?> Extensions { get; set; } = new();
}
