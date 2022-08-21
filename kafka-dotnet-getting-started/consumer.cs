using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using System.Collections.Generic;
class Consumer {

    static void Main(string[] args)
    {
        if (args.Length != 1) {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0], true)
            .Build();
        
        //if(configuration[""])
        if(string.IsNullOrEmpty(configuration["bootstrap.servers"]))
            configuration["bootstrap.servers"]="localhost:9092";
        configuration["group.id"] = "kafka-dotnet-getting-started";
        configuration["auto.offset.reset"] = "earliest";
        string groupInstanceId = "";
        if(args.Length > 1){
            configuration["group.instance.id"] = args[1];
            groupInstanceId = args[1];
        }

        const string topic = "purchases";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        int count = 2;
        List<Task> taskList = new List<Task>();
        for(int i = 0; i < count; i++){
            Dictionary<string, string> config = new Dictionary<string, string>(){
                ["bootstrap.servers"]="localhost:9092",
                ["group.id"] = "kafka-dotnet-getting-started",
                ["auto.offset.reset"] = "earliest",
                ["group.instance.id"] = groupInstanceId + i.ToString()
            };
            var k=i;
            var t = Task.Run(()=>{
                //using (var consumer = new ConsumerBuilder<string, string>(
                //    configuration.AsEnumerable()).Build())

                using(var consumer = new ConsumerBuilder<string, string>(config).Build())
                {
                    consumer.Subscribe(topic);
                    try {
                        while (true) {
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine($"Consumed {topic} {k} with {cr.Message.Key,-10} : {cr.Message.Value}");
                            Thread.Sleep(200);
                        }
                    }
                    catch (OperationCanceledException) {
                        // Ctrl-C was pressed.
                    }
                    finally{
                        consumer.Close();
                    }
                }
            });
            taskList.Add(t);
        }
        Task.WaitAll(taskList.ToArray());
        
    }
}