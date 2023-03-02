using Confluent.Kafka;
using Domain;
using Newtonsoft.Json;


var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
};

CancellationTokenSource token = new();

var producer = new ProducerBuilder<Null, string>(config).Build();

try
{
    while (Console.ReadLine() != null)
    {
        var response = await producer.ProduceAsync("test-topic", new Message<Null, string> { Value = JsonConvert.SerializeObject(new SomeEntity(1, Console.ReadLine())) });
        Console.WriteLine($"Partion: {response.Partition} - Message: {response.Message}");
    }
}
catch (ProduceException<Null, string> ex)
{
    Console.WriteLine(ex.Message);
}