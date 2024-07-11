using System.Threading.Tasks.Sources;

namespace CloudEventDotNet;

/// <summary>
/// 该 WorkItemWaiter 类的主要功能是提供一种手动控制异步任务完成的机制。
/// 通过实现 IValueTaskSource 接口，该类允许用户手动控制任务（ValueTask）的完成、状态检查和回调注册。
/// 这在需要手动触发异步操作完成的场景中非常有用。
/// 一个可以将同步任务、不同线程同步操作，通过状态机构建异步方法
/// </summary>
internal class WorkItemWaiter : IValueTaskSource //IValueTaskSource 接口，这是一个用于自定义 ValueTask 行为的接口
{
    //ManualResetValueTaskSourceCore 是一个结构体，提供了一种手动控制 ValueTask 完成的机制
    //ManualResetValueTaskSourceCore 实现了IValueTaskSource接口，并提供了一些方法来控制 ValueTask 的状态、结果、回调等。
    private ManualResetValueTaskSourceCore<bool> _tcs;

    //这个方法会在任务完成时抛出异常或返回结果
    //实现接口，告诉调用者，任务是否已经完成，以及是否有结果，是否有异常等
    public void GetResult(short token) => _tcs.GetResult(token);

    //获取任务的当前状态
    // Canceled  3 操作因取消操作而完成。
    // Faulted   2 操作已完成但有错误。
    // Pending   0 操作尚未完成。
    // Succeeded 1 操作已成功完成。
    public ValueTaskSourceStatus GetStatus(short token) => _tcs.GetStatus(token);

    //接受一个回调方法 continuation、一个状态对象 state、一个短整数 token 和一个 ValueTaskSourceOnCompletedFlags 枚举值 flags。这个方法用来注册当任务完成时要执行的回调
    //实现延续
    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _tcs.OnCompleted(continuation, state, token, flags);
    //实现状态机，能够控制此任务是否已经完成，以及是否有异常
    public void SetResult() => _tcs.SetResult(true);

    //该任务与当前 WorkItemWaiter 关联，并且使用 _tcs 的版本号
    public ValueTask Task => new(this, _tcs.Version);
}
