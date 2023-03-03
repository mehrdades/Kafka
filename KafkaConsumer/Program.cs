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

var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("test-topic");

Console.WriteLine("Enter the Partition Id: ");
var topicPartiotion = new TopicPartition("test-topic", new Confluent.Kafka.Partition(Convert.ToInt32(Console.ReadLine())));
consumer.Assign(topicPartiotion);

Console.WriteLine($"{Environment.NewLine}Listening...");

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