namespace CloudEventDotNet;

/// <summary>
/// CloudEvents元数据.
/// </summary>
/// <param name="PubSubName"></param>
/// <param name="Topic"></param>
/// <param name="Type"></param>
/// <param name="Source"></param>
public record struct CloudEventMetadata(
    string PubSubName,
    string Topic,
    string Type,
    string Source
);
