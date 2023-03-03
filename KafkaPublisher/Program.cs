using Confluent.Kafka;
using KafkaPublisher;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var bootstrapServers = builder.Configuration.GetSection("AppSettings")["BootstrapServers"];
var config = new ProducerConfig { BootstrapServers = bootstrapServers };
builder.Services.AddSingleton<IProducer<Null, string>>(x => new ProducerBuilder<Null, string>(config).Build());

builder.Services.AddSingleton<ISomeEntityPublisher,SomeEntityPublisher>();  

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
