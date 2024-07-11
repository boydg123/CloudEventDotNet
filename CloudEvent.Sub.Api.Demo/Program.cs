using CloudEvent.Sub.Api.Demo.CloudEvent;
using CloudEventDotNet;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();

builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "CloudEvent.Pub.Api.Demo", Version = "v1" });
});

builder.Services.AddCloudEvents(defaultPubSubName: "kafka", defaultTopic: "derrick").Load(typeof(AddWorkWxEvent).Assembly).AddKafkaPubSub("kafka", null, options =>
{
    options.ConsumerConfig = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "DerrickGroup",
        AutoOffsetReset = AutoOffsetReset.Latest,

        QueuedMinMessages = 300_000,
        FetchWaitMaxMs = 1_000
    };
    options.RunningWorkItemLimit = 128;
    options.DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce;
});

var app = builder.Build();

app.MapControllers();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "My API V1"));
}

app.Run();
