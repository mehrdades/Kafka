using Confluent.Kafka;
using Domain;
using Newtonsoft.Json;

namespace KafkaPublisher
{
    public class SomeEntityPublisher : ISomeEntityPublisher
    {
        private readonly IProducer<Null, string> _producer;

        public SomeEntityPublisher(IProducer<Null, string> producer)
        {
            _producer = producer;
        }

        public async Task ProduceAsync(KafkaMessage<SomeEntity> kafkaMessage) =>
            await _producer.ProduceAsync(kafkaMessage.TopicPartition, new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(kafkaMessage.Data)
            });
    }
}
