using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

/// <summary>
/// Redis 消息工作项上下文。
/// 封装了事件注册表、依赖注入作用域工厂、Redis 数据库实例和日志工厂，供消息处理时使用。
/// </summary>
/// <param name="Registry">事件注册表</param>
/// <param name="ScopeFactory">依赖注入作用域工厂</param>
/// <param name="Redis">Redis 数据库实例</param>
/// <param name="LoggerFactory">日志工厂</param>
internal record RedisWorkItemContext(
    Registry Registry,
    IServiceScopeFactory ScopeFactory,
    IDatabase Redis,
    ILoggerFactory LoggerFactory);
