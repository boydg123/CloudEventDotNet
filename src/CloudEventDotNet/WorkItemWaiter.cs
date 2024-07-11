using System.Threading.Tasks.Sources;

namespace CloudEventDotNet;

/// <summary>
/// �� WorkItemWaiter �����Ҫ�������ṩһ���ֶ������첽������ɵĻ��ơ�
/// ͨ��ʵ�� IValueTaskSource �ӿڣ����������û��ֶ���������ValueTask������ɡ�״̬���ͻص�ע�ᡣ
/// ������Ҫ�ֶ������첽������ɵĳ����зǳ����á�
/// һ�����Խ�ͬ�����񡢲�ͬ�߳�ͬ��������ͨ��״̬�������첽����
/// </summary>
internal class WorkItemWaiter : IValueTaskSource //IValueTaskSource �ӿڣ�����һ�������Զ��� ValueTask ��Ϊ�Ľӿ�
{
    //ManualResetValueTaskSourceCore ��һ���ṹ�壬�ṩ��һ���ֶ����� ValueTask ��ɵĻ���
    //ManualResetValueTaskSourceCore ʵ����IValueTaskSource�ӿڣ����ṩ��һЩ���������� ValueTask ��״̬��������ص��ȡ�
    private ManualResetValueTaskSourceCore<bool> _tcs;

    //������������������ʱ�׳��쳣�򷵻ؽ��
    //ʵ�ֽӿڣ����ߵ����ߣ������Ƿ��Ѿ���ɣ��Լ��Ƿ��н�����Ƿ����쳣��
    public void GetResult(short token) => _tcs.GetResult(token);

    //��ȡ����ĵ�ǰ״̬
    // Canceled  3 ������ȡ����������ɡ�
    // Faulted   2 ��������ɵ��д���
    // Pending   0 ������δ��ɡ�
    // Succeeded 1 �����ѳɹ���ɡ�
    public ValueTaskSourceStatus GetStatus(short token) => _tcs.GetStatus(token);

    //����һ���ص����� continuation��һ��״̬���� state��һ�������� token ��һ�� ValueTaskSourceOnCompletedFlags ö��ֵ flags�������������ע�ᵱ�������ʱҪִ�еĻص�
    //ʵ������
    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _tcs.OnCompleted(continuation, state, token, flags);
    //ʵ��״̬�����ܹ����ƴ������Ƿ��Ѿ���ɣ��Լ��Ƿ����쳣
    public void SetResult() => _tcs.SetResult(true);

    //�������뵱ǰ WorkItemWaiter ����������ʹ�� _tcs �İ汾��
    public ValueTask Task => new(this, _tcs.Version);
}
