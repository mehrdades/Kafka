using Confluent.Kafka;

namespace KafkaPublisher
{
    public class KafkaMessage<T>
    {
        public TopicPartition TopicPartition { get; set; }

        public T Data { get; set; }
    }
}
