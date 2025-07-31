using CloudEvent.Pub.Api.Demo.CloudEvent;
using CloudEventDotNet;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "CloudEvent.Pub.Api.Demo", Version = "v1" });
});

builder.Services.AddCloudEvents(defaultPubSubName: "kafka1", defaultTopic: "derrick").Load(typeof(AddWorkWxEvent).Assembly).AddKafkaPubSub("kafka1", options =>
{
    options.ProducerConfig = new Confluent.Kafka.ProducerConfig
    {
        BootstrapServers = "host.docker.internal:9092",
        Acks = Confluent.Kafka.Acks.Leader,
        LingerMs = 10
    };
}, null);

var app = builder.Build();

app.UseHttpsRedirection();

app.MapControllers();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "My API V1"));
}

app.Run();
