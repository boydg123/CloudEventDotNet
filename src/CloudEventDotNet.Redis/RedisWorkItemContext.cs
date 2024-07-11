using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis工作项上下文
/// </summary>
/// <param name="Registry"></param>
/// <param name="ScopeFactory"></param>
/// <param name="Redis"></param>
/// <param name="RedisTelemetry"></param>
internal record RedisWorkItemContext(
    Registry Registry,
    IServiceScopeFactory ScopeFactory,
    IDatabase Redis,
    RedisMessageTelemetry RedisTelemetry);
