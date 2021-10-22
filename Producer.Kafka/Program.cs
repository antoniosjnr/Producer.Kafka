using Confluent.Kafka;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace Producer.Kafka
{
    class Program
    {
        public static KafkaConfig KafkaConfig
        {
            get
            {
                var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: false);

                IConfiguration config = builder.Build();

                return config.GetSection("KafkaConfig").Get<KafkaConfig>();
            }
        }

        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            logger.Information("Testando o envio de mensagens com Kafka");

            if (string.IsNullOrWhiteSpace(KafkaConfig.Url))
            {
                logger.Error(
                    "Informações do Kafka não configuradas! Favor ajustar o appsettings.json.");
                return;
            }

            logger.Information($"BootstrapServers = {KafkaConfig.Url}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = KafkaConfig.Url
                };

                using (var producer = new ProducerBuilder<string, Order>(config)
                    .SetValueSerializer(new DataSerializer<Order>())
                    .Build())
                {
                    logger.Information($"Topic = tp_1");

                    for (int i = 1; i < 2; i++)
                    {
                        var result1 = await producer.ProduceAsync(
                            "tp_1",
                            new Message<string, Order>
                            {
                                Value = new Order() { Amount = new decimal(10 * i), Number = $"00{i}"},
                                Key = Guid.NewGuid().ToString()
                            });

                        logger.Information(
                            $"Mensagem: Mensagem {i} | " +
                            $"Status: {result1.Status.ToString()}");
                    }
                }

                using (var producer = new ProducerBuilder<string, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(
                        "tp_2",
                        new Message<string, string>
                            {Value = "Mensagem 2"});

                    logger.Information(
                        $"Mensagem: Mensagem 2 | " +
                        $"Status: {result.Status.ToString()}");
                }

                logger.Information("Concluído o envio de mensagens");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}