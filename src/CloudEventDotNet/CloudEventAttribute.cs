namespace CloudEventDotNet;

/// <summary>
/// CloudEvent���
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class CloudEventAttribute : Attribute
{
    /// <summary>
    /// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#type">'type'</see> attribute.
    /// Type of occurrence which has happened.
    /// Often this attribute is used for routing, observability, policy enforcement, etc.
    /// CloudEvent���ͣ�������·�ɣ��ɹ۲��ԣ�����ǿ�Ƶȡ�
    /// </summary>
    public string? Type { get; init; }

    /// <summary>
    /// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#source">'source'</see> attribute.
    /// CloudEventԴ�������¼��Ĳ����ߡ�ͨ�������¼�Դ�����ͣ���֯�����¼����¼������Ĺ��̣��Լ�һЩΨһ��ʶ����
    /// </summary>
    /// <remarks>
    /// ��<see cref="PubSubBuilder"/>����defaultSource��ΪĬ��Դ.
    /// </remarks>
    public string? Source { get; init; }

    /// <summary>
    /// CloudEvents�����͵���PubSub������
    /// </summary>
    /// <remarks>
    /// ��<see cref="PubSubBuilder"/>�е�����defaultPubSubName Ĭ������.
    /// </remarks>
    public string? PubSubName { get; init; }

    /// <summary>
    /// CloudEvents�����͵�������
    /// </summary>
    /// <remarks>
    /// �� <see cref="PubSubBuilder"/>��������ʹ��defaultTopic ����Ĭ������.
    /// </remarks>
    public string? Topic { get; init; }
}
