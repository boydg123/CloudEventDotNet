using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis工作项上下文
/// </summary>
/// <param name="Registry"></param>
/// <param name="ScopeFactory"></param>
/// <param name="Redis"></param>
/// <param name="LoggerFactory"></param>
internal record RedisWorkItemContext(
    Registry Registry,
    IServiceScopeFactory ScopeFactory,
    IDatabase Redis,
    ILoggerFactory LoggerFactory);
