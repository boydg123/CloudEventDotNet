using System.ComponentModel.DataAnnotations;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis PubSub ѡ��
/// </summary>
public abstract class RedisPubSubOptions
{
    /// <summary>
    /// һ�����ڽ��� Redis ����������ʹ�õ� IConnectionMultiplexer �Ĺ�����
    /// </summary>
    [Required]
    public Func<IConnectionMultiplexer> ConnectionMultiplexerFactory { get; set; } = default!;

    /// <summary>
    /// ���ݿ�
    /// </summary>
    public int Database { get; set; }
}

/// <summary>
/// Redis ����ѡ��
/// </summary>
public class RedisPublishOptions : RedisPubSubOptions
{
    /// <summary>
    /// ������е���Ŀ���� ��ָ���ĳ��ȴﵽʱ���ɵ���Ŀ���Զ���̭���Ա������Ĵ�Сʼ�ձ��ֲ��䡣 Ĭ��Ϊ���ޡ�
    /// </summary>
    public int? MaxLength { get; set; }
}

/// <summary>
/// Redis ����ѡ��
/// </summary>
public class RedisSubscribeOptions : RedisPubSubOptions
{
    /// <summary>
    /// �����ߵ���������
    /// </summary>
    [Required]
    public string ConsumerGroup { get; set; } = default!;

    /// <summary>
    /// ÿ����ȥ����� CloudEvent ����
    /// </summary>
    public int PollBatchSize { get; set; } = 100;

    /// <summary>
    /// ��ѯ��CloudEvent�ļ��ʱ�䣬Ĭ��15��
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// ���ؽ��̶�����δ�����CloudEvents�����ơ�
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 128;

    /// <summary>
    /// The amount time a CloudEvent must be pending before attempting to redeliver it. Defaults to "60s".
    /// CloudEvent�ڳ������´���֮ǰ��������ʱ�䡣Ĭ��ֵΪ��60s��
    /// </summary>
    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromSeconds(60);
}
