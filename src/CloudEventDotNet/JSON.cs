using System.Text.Json;
using System.Text.Json.Serialization;

namespace CloudEventDotNet;

/// <summary>
/// JSON 工具类。
/// 提供 CloudEventDotNet 内部统一的 JSON 序列化和反序列化方法。
/// </summary>
internal static class JSON
{
    /// <summary>
    /// 默认的 JsonSerializerOptions，配置了常用选项。
    /// </summary>
    public static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        //Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    /// <summary>
    /// 序列化对象为 JSON 字符串。
    /// </summary>
    public static string Serialize<T>(T value, JsonSerializerOptions? options = null)
        => JsonSerializer.Serialize(value, options ?? DefaultJsonSerializerOptions);
    public static byte[] SerializeToUtf8Bytes<T>(T value, JsonSerializerOptions? options = null)
        => JsonSerializer.SerializeToUtf8Bytes(value, options ?? DefaultJsonSerializerOptions);

    /// <summary>
    /// 反序列化 JSON 字符串为对象。
    /// </summary>
    public static T? Deserialize<T>(string value, JsonSerializerOptions? options = null)
        => JsonSerializer.Deserialize<T>(value, options ?? DefaultJsonSerializerOptions);
    public static T? Deserialize<T>(ReadOnlySpan<byte> value, JsonSerializerOptions? options = null)
        => JsonSerializer.Deserialize<T>(value, options ?? DefaultJsonSerializerOptions);
}
