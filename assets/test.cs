using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading.Tasks;

class Worker
{
    public string NodeRole { get; set; }

    public Worker(string nodeRole)
    {
        NodeRole = nodeRole;
    }

    public async Task StartListeningAsync()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" }; // Change host if needed
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            string[] queues = { "A", "B", "C", "D", "E" };

            while (true)
            {
                foreach (var queue in queues)
                {
                    if (!NodeRole.Contains(queue))
                    {
                        Console.WriteLine($"Skipping queue {queue} as it's not in the role.");
                        continue;
                    }

                    channel.QueueDeclare(queue: queue, durable: false, exclusive: false, autoDelete: false, arguments: null);

                    var result = channel.BasicGet(queue, true); // true for auto-acknowledge
                    if (result != null)
                    {
                        var message = Encoding.UTF8.GetString(result.Body.ToArray());
                        Console.WriteLine($" [x] Received '{message}' from queue {queue}");

                        // Here, handle the message as needed

                        break; // Break out of the foreach loop to start from queue A again
                    }
                }

                // Optional: Add a delay between iterations to reduce CPU usage
                await Task.Delay(1000);
            }
        }
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        string nodeRole = "A C E"; // Set this based on the node's role

        var worker = new Worker(nodeRole);
        await worker.StartListeningAsync();
    }
}
