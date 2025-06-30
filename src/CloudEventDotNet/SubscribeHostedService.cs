using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

/// <summary>
/// CloudEvent 订阅后台服务。
/// 负责在应用启动时自动启动所有已注册的 ICloudEventSubscriber 实例，实现事件的自动拉取与分发。
/// </summary>
public class SubscribeHostedService : IHostedService
{
    // PubSub 配置选项
    private readonly PubSubOptions _options;
    // 所有订阅者实例
    private readonly List<ICloudEventSubscriber> _subscribers;
    // 日志实例
    private readonly ILogger<SubscribeHostedService> _logger;

    /// <summary>
    /// 构造函数，注入依赖项。
    /// </summary>
    /// <param name="logger">日志</param>
    /// <param name="serviceProvider">依赖注入容器</param>
    /// <param name="options">发布/订阅配置</param>
    public SubscribeHostedService(
        ILogger<SubscribeHostedService> logger,
        IServiceProvider serviceProvider,
        IOptions<PubSubOptions> options)
    {
        _options = options.Value;
        // 通过工厂方法创建所有已注册的订阅者实例
        _subscribers = _options
            .SubscriberFactoris.Values
            .Select(factory => factory(serviceProvider))
            .ToList();
        _logger = logger;
    }

    /// <summary>
    /// 启动所有订阅者。
    /// 应用启动时自动调用，拉取消息并分发给处理器。
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting subscribers");
        await Task.WhenAll(_subscribers.Select(s => s.StartAsync()));
        _logger.LogInformation("Started subscribers");
    }

    /// <summary>
    /// 停止所有订阅者。
    /// 应用关闭时自动调用，优雅停止消息拉取。
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stoping subscribers");
        await Task.WhenAll(_subscribers.Select(s => s.StopAsync()));
        _logger.LogInformation("Stopped subscribers");
    }
}
