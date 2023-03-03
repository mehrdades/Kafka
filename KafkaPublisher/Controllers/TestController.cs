using Domain;
using Microsoft.AspNetCore.Mvc;

namespace KafkaPublisher.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TestController : ControllerBase
    {
        private readonly ISomeEntityPublisher _someEntityPublisher;
        private readonly IConfiguration _configuration;

        public TestController(ISomeEntityPublisher someEntityPublisher, IConfiguration configuration)
        {
            _someEntityPublisher = someEntityPublisher;
            _configuration = configuration;
        }

        [HttpPost]
        public async Task Post([FromBody] SomeEntity someEntity, int partitionId)
        {
            var topic = _configuration.GetValue<string>("AppSettings:Topic");
            var kafkaMessage = new KafkaMessage<SomeEntity>()
            {
                TopicPartition = new Confluent.Kafka.TopicPartition(topic, new Confluent.Kafka.Partition(partitionId)),
                Data = someEntity
            };
            await _someEntityPublisher.ProduceAsync(kafkaMessage);
        }
    }
}
