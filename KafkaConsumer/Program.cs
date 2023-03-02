using Confluent.Kafka;
using Domain;
using Newtonsoft.Json;


var config = new ConsumerConfig
{
    GroupId = "test-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

CancellationTokenSource token = new();

Console.ReadLine();
var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("test-topic");


try
{
    while (true)
    {
        var response = consumer.Consume(token.Token);
        if (response.Message is not null)
        {
            var someEntity = JsonConvert.DeserializeObject<SomeEntity>(response.Message.Value);
            Console.WriteLine($"Message is: Id: {someEntity.Id} - Name: {someEntity.Name}");
        }
    }
}
catch (Exception)
{

    throw;
}