using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace CloudEventDotNet;
/// <summary>
/// CloudEvent 事件处理器基类。
/// 封装了事件元数据和处理委托，便于统一调度和调用。
/// </summary>
internal class CloudEventHandler
{
    /// <summary>
    /// 事件元数据。
    /// </summary>
    public CloudEventMetadata Metadata { get; }
    /// <summary>
    /// 事件处理委托。
    /// </summary>
    private readonly HandleCloudEventDelegate _handler;
    // 日志工厂
    private readonly ILoggerFactory _loggerFactory;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;

    public CloudEventHandler(CloudEventMetadata metadata, HandleCloudEventDelegate handler, IServiceScopeFactory scopeFactory, ILoggerFactory loggerFactory)
    {
        Metadata = metadata;
        _handler = handler;
        _scopeFactory = scopeFactory;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<CloudEventHandler>();
    }

    /// <summary>
    /// 调用事件处理委托。
    /// </summary>
    /// <param name="cloudEvent">原始 CloudEvent</param>
    /// <param name="token">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task<bool> HandleAsync(CloudEvent cloudEvent, CancellationToken token)
    {
        _logger.LogDebug($"OnProcessing:{JSON.Serialize(cloudEvent)}");
        try
        {
            using var scope = _scopeFactory.CreateScope();
            await _handler(scope.ServiceProvider, cloudEvent, token).ConfigureAwait(false);
            _logger.LogDebug($"Process CloudEvent {cloudEvent.Id}");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to process CloudEvent {cloudEvent.Id}");
            return false;
        }
    }
}
