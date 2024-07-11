using CloudEventDotNet;

namespace CloudEvent.Pub.Api.Demo.CloudEvent;

[CloudEvent]
public class AddWorkWxEvent
{
    /// <summary>
    /// 用户ID
    /// </summary>
    public string? UserId { get; set; }

    /// <summary>
    /// 邀约ID
    /// </summary>
    public string? InviterId { get; set; }
}
