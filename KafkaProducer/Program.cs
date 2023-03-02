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
    var message = Console.ReadLine();
    var i = 0;  
    do
    {
        i++;
        var response = await producer.ProduceAsync("test-topic", new Message<Null, string> { Value = JsonConvert.SerializeObject(new SomeEntity(i, message)) });
        Console.WriteLine($"Partion: {response.Partition} - Message: {response.Message}");
        message = Console.ReadLine();
    } while (message is not null);
}
catch (ProduceException<Null, string> ex)
{
    Console.WriteLine(ex.Message);
}