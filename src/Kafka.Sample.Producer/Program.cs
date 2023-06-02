using Bogus;
using Bogus.Extensions.Brazil;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Sample;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json")
    .Build();

var adminClientConfig = configuration.GetSection("AdminClient").Get<AdminClientConfig>().ThrowIfContainsNonUserConfigurable();
var producerConfig = configuration.GetSection("Producer").Get<ProducerConfig>().ThrowIfContainsNonUserConfigurable();
var topicName = configuration.GetValue<string>("General:TopicName");

var adminClientBuilder = new AdminClientBuilder(adminClientConfig)
    .SetErrorHandler((_, e) => Console.WriteLine($"Admin Error: {e.Reason}"));

var producerBuilder = new ProducerBuilder<string, User>(producerConfig)
    .SetKeySerializer(Serializers.Utf8)
    .SetValueSerializer(new GenericJsonSerializer<User>())
    .SetErrorHandler((_, e) => Console.WriteLine($"Producer Error: {e.Reason}"));

var container = new ServiceCollection()
    .AddSingleton(producerBuilder.Build())
    .AddSingleton(adminClientBuilder.Build())
    .BuildServiceProvider();

var admin = container.GetService<IAdminClient>();

try
{
    await admin.CreateTopicsAsync(new TopicSpecification[]
    {
        new TopicSpecification
        {
            Name = topicName,
            ReplicationFactor = 1,
            NumPartitions = 1
        }
    });

    Console.WriteLine($"Topic {topicName} is created.");
}
catch (Exception)
{
    Console.WriteLine($"Topic {topicName} is already created.");
}

var producer = container.GetService<IProducer<string, User>>();

while (true)
{
    for (int i = 0; i < 10; i++)
    {
        try
        {
            var faker = new Faker<User>()
                .RuleFor(r => r.Id, r => r.Person.Cpf())
                .RuleFor(r => r.Name, r => r.Person.FullName);

            var user = new User();

            faker.Populate(user);

            var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, User>()
            {
                Key = user.Id,
                Value = user
            });

            Console.WriteLine($"User id: {user.Id}, user name: {user.Name} delivered to: {deliveryReport.TopicPartitionOffset}");
        }
        catch (ProduceException<string, string> e)
        {
            Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
        }
    }

    Console.WriteLine();
    Console.WriteLine("Press any key to send more 10 user to topic ...");
    Console.ReadKey();
}
