using Confluent.Kafka;

namespace CloudEventDotNet;

/// <summary>
/// Kafka����������
/// </summary>
public class KafkaPublishOptions
{
    /// <summary>
    /// Kafka����������.
    /// </summary>
    public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig();
}

/// <summary>
/// Kafka����������
/// </summary>
public class KafkaSubscribeOptions
{
    /// <summary>
    /// Kafka����������.
    /// </summary>
    public ConsumerConfig ConsumerConfig { get; set; } = new ConsumerConfig();

    /// <summary>
    /// ��Ϣ����֤. Ĭ����AtMostOnce
    /// </summary>
    public DeliveryGuarantee DeliveryGuarantee { get; set; } = DeliveryGuarantee.AtMostOnce;

    /// <summary>
    /// ���ؽ��̶�����δ�����CloudEvents������.Ĭ��1024
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 1024;
}

public enum DeliveryGuarantee
{
    AtMostOnce, AtLeastOnce
}
