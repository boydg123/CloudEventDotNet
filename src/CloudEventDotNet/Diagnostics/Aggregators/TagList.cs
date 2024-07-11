namespace CloudEventDotNet.Diagnostics.Aggregators;

public record struct TagList(
    string? Name1 = default,
    object? Value1 = default,
    string? Name2 = default,
    object? Value2 = default,
    string? Name3 = default,
    object? Value3 = default,
    string? Name4 = default,
    object? Value4 = default)
{
    public static readonly TagList Empty = new();

    /// <summary>
    /// 将TagList转换为KeyValuePair数组
    /// </summary>
    /// <returns></returns>
    public KeyValuePair<string, object?>[] ToArray()
    {
        // 使用模式匹配 (switch) 来检查每个字段是否为null
        // 根据匹配结果创建适当大小的KeyValuePair数组
        return this switch
        {
            (null, _, _, _, _, _, _, _) => Array.Empty<KeyValuePair<string, object?>>(),
            (_, _, null, _, _, _, _, _) => [new KeyValuePair<string, object?>(Name1!, Value1)],
            (_, _, _, _, null, _, _, _) => new[]
            {
                new KeyValuePair<string, object?>(Name1!, Value1),
                new KeyValuePair<string, object?>(Name2!, Value2)
            },
            (_, _, _, _, _, _, null, _) => new[]
            {
                new KeyValuePair<string, object?>(Name1!, Value1),
                new KeyValuePair<string, object?>(Name2!, Value2),
                new KeyValuePair<string, object?>(Name3!, Value3)
            },
            (_, _, _, _, _, _, _, _) => new[]
            {
                new KeyValuePair<string, object?>(Name1!, Value1),
                new KeyValuePair<string, object?>(Name2!, Value2),
                new KeyValuePair<string, object?>(Name3!, Value3),
                new KeyValuePair<string, object?>(Name4!, Value4)
            },
        };
    }
};
