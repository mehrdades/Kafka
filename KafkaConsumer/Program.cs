using Confluent.Kafka;
using Domain;
using Newtonsoft.Json;
using System.Collections.Concurrent;

class Program
{

    static async Task Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };


        //CancellationTokenSource token = new();

        //var consumer = new ConsumerBuilder<Null, string>(config).Build();

        //consumer.Subscribe("test-topic");

        //Console.WriteLine("Enter the Partition Id: ");
        //var topicPartiotion = new TopicPartition("test-topic", new Confluent.Kafka.Partition(Convert.ToInt32(Console.ReadLine())));
        //consumer.Assign(topicPartiotion);

        //Console.WriteLine($"{Environment.NewLine}Listening...");

        //try
        //{
        //    while (true)
        //    {
        //        var response = consumer.Consume(token.Token);
        //        _ = HandleMessageAsync(response);
        //    }
        //}
        //catch (Exception)
        //{

        //    throw;
        //}



        //static async Task HandleMessageAsync(ConsumeResult<Null, string> consumeResult)
        //{
        //    await Task.Delay(10000);

        //    await Task.Run(() =>
        //    {
        //        if (consumeResult.Message is not null)
        //        {
        //            var someEntity = JsonConvert.DeserializeObject<SomeEntity>(consumeResult.Message.Value);
        //            Console.WriteLine($"Message is: Id: {someEntity.Id} - Name: {someEntity.Name}");
        //        }
        //    });
        //}


        using (var consumer = new ConsumerBuilder<string, string>(config).Build())
        {
            ConcurrentDictionary<string, Task<bool>> messageProcessingTasks = new ConcurrentDictionary<string, Task<bool>>();

            consumer.Subscribe("test-topic");

            Console.WriteLine("Enter the Partition Id: ");
            var topicPartiotion = new TopicPartition("test-topic", new Partition(Convert.ToInt32(Console.ReadLine())));
            consumer.Assign(topicPartiotion);

            CancellationTokenSource cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // Prevent the process from exiting immediately
                cts.Cancel();
            };
            var messageProcessor = new MessageProcessor(consumer);

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Reached end of partition: {consumeResult.TopicPartitionOffset}");
                            continue;
                        }
                        // Process the received record asynchronously in the background
                        messageProcessor.ProcessRecordInBackground(consumeResult);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error consuming message: {e.Error.Reason}");
                        SendAlert("Error consuming message", e.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Consumer is being canceled, handle cleanup here if necessary
            }
            finally
            {
                consumer.Close();
            }
        }
    }
    static void SendAlert(string subject, string message)
    {
        Console.WriteLine($"ALERT: {subject} - {message}");
    }

    class MessageProcessor
    {
        private ConcurrentDictionary<string, Task<bool>> messageProcessingTasks = new ConcurrentDictionary<string, Task<bool>>();
        private IConsumer<string, string> consumer;
        private object commitLock = new object();
        private const int MaxBatchSize = 0; // batch size for committing messages
        private List<TopicPartitionOffset> processedOffsets = new List<TopicPartitionOffset>();

        public MessageProcessor(IConsumer<string, string> consumer)
        {
            this.consumer = consumer;
        }

        public void ProcessRecordInBackground(ConsumeResult<string, string> message)
        {
            var task = Task.Run(async () =>
            {
                int retryCount = 0;
                bool isSuccess = false;

                while (!isSuccess && retryCount <= 3) // Retry up to 3 times
                {
                    isSuccess = await ProcessRecordAsync(message);

                    if (!isSuccess)
                    {
                        Console.WriteLine($"Retrying message: {message.Message.Value}, Retry Count: {retryCount}");
                        SendAlert($"Retrying message: {message.Value}", $"Retry Count: {retryCount}");
                        retryCount++;
                    }
                }
                // Update message tracking status based on processing result
                UpdateMessageStatus(message.TopicPartitionOffset, isSuccess);
                return isSuccess;
            });

            if (message.Message.Key is not null)
                messageProcessingTasks.TryAdd(message.Message.Key, task);
        }

        private async Task<bool> ProcessRecordAsync(ConsumeResult<string, string> message)
        {
            Console.WriteLine();
            // Simulate processing failure for demonstration
            if (message.Message.Value.Contains("fail"))
            {
                await Task.Delay(10000);
                Console.WriteLine($"Failed to process message: {message.Message.Value}");
                return false;
            }

            await Task.Delay(10000); // Simulating some asynchronous work
            var someEntity = JsonConvert.DeserializeObject<SomeEntity>(message.Message.Value);
            Console.WriteLine($"Message is: Id: {someEntity.Id} - Name: {someEntity.Name}");
            //Console.WriteLine($"Processed message: {message.Message.Value}");

            return true;
        }

        private void UpdateMessageStatus(TopicPartitionOffset offset, bool isSuccess)
        {
            // Update message tracking status in your database
            // Set the isSuccess status for the given messageId
            Console.WriteLine();
            Console.WriteLine($"Message at offset '{offset}' status updated: Success={isSuccess}");
            messageProcessingTasks.TryRemove(offset.TopicPartition.ToString(), out _);

            lock (commitLock)
            {
                processedOffsets.Add(offset);

                if (processedOffsets.Count >= MaxBatchSize)
                {
                    // Manually commit the batch of processed offsets
                    consumer.Commit(processedOffsets);
                    processedOffsets.Clear();
                }
            }
        }
        private void SendAlert(string subject, string message)
        {
            Console.WriteLine($"ALERT: {subject} - {message}");
        }
    }
}