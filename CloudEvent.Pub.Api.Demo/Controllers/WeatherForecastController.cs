using CloudEvent.Pub.Api.Demo.CloudEvent;
using CloudEventDotNet;
using Microsoft.AspNetCore.Mvc;

namespace CloudEvent.Pub.Api.Demo.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IEnumerable<WeatherForecast> Get()
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <param name="pubsub"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        [HttpPost("SendMessage")]
        public async Task<AddWorkWxEvent> SendMessageAsync([FromServices] ICloudEventPubSub pubsub, [FromBody] AddWorkWxEvent data)
        {
            await pubsub.PublishAsync(data);
            return data;
        }
    }
}
