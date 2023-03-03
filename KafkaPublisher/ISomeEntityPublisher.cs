using Domain;

namespace KafkaPublisher
{
    public interface ISomeEntityPublisher
    {
        Task ProduceAsync(KafkaMessage<SomeEntity> kafkaMessage);
    }
}