using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;

class Producer {
    static void Main(string[] args)
    {
        if (args.Length != 1) {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0], true)
            .Build();

        if(string.IsNullOrEmpty(configuration["bootstrap.servers"]))
            configuration["bootstrap.servers"]="localhost:9092";
        const string topic = "purchases";

        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };
        Console.WriteLine("Build Producer");
        using (var producer = new ProducerBuilder<string, string>(
            configuration.AsEnumerable()).Build())
        {
            Console.WriteLine("Begin producing message");
            var numProduced = 0;
            //const int numMessages = 10;
            //for (int i = 0; i < numMessages; ++i)
            int i = 0;
            while(true)
            {
                Random rnd = new Random();
                var user = users[rnd.Next(users.Length)] + $"{i}";
                var item = items[rnd.Next(items.Length)] + $"{i}";
                i++;
                producer.Produce(topic, new Message<string, string> { Key = user , Value = item },
                    (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError) {
                            Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                        }
                        else {
                            Console.WriteLine($"topic {topic}: key = {user,-10} value = {item}");
                            numProduced += 1;
                        }
                    });
                System.Threading.Thread.Sleep(10);
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }
}