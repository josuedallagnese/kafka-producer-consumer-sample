using Confluent.Kafka;
using Kafka.Sample;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json")
    .Build();

var consumerConfig = configuration.GetSection("Consumer").Get<ConsumerConfig>().ThrowIfContainsNonUserConfigurable();
var topicName = configuration.GetValue<string>("General:TopicName");
var consumerTimeout = configuration.GetValue<TimeSpan>("General:ConsumerTimeout");

consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
consumerConfig.EnableAutoCommit = false;
consumerConfig.EnableAutoOffsetStore = true;

var consumerBuilder = new ConsumerBuilder<string, User>(consumerConfig)
    .SetKeyDeserializer(Deserializers.Utf8)
    .SetValueDeserializer(new GenericJsonSerializer<User>())
    .SetErrorHandler((_, e) => Console.WriteLine($"Consumer Error: {e.Reason}"));

var container = new ServiceCollection()
    .AddSingleton(consumerBuilder.Build())
    .BuildServiceProvider();

var consumer = container.GetService<IConsumer<string, User>>();

consumer.Subscribe(topicName);

Console.WriteLine($"Subscribe {topicName} ...");

while (true)
{
    var consumeResult = consumer.Consume();

    if (consumeResult != null)
    {
        var user = consumeResult.Message.Value;

        Console.WriteLine($"User id: {user.Id}, user name: {user.Name}");

        try
        {
            consumer.Commit(consumeResult);

            Console.WriteLine($"Commit offset.");
        }
        catch (KafkaException e)
        {
            Console.WriteLine($"Commit error: {e.Error.Reason}");
        }
    }
}
