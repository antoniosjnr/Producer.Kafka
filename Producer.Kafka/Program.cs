using System;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
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

            if (string.IsNullOrWhiteSpace(KafkaConfig.Topic) || string.IsNullOrWhiteSpace(KafkaConfig.Url))
            {
                logger.Error(
                    "Informações do Kafka não configuradas! Favor ajustar o appsettings.json.");
                return;
            }

            logger.Information($"BootstrapServers = {KafkaConfig.Url}");
            logger.Information($"Topic = {KafkaConfig.Topic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = KafkaConfig.Url
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 0; i < args.Length; i++)
                    {
                        var result = await producer.ProduceAsync(
                            KafkaConfig.Topic,
                            new Message<Null, string>
                                { Value = args[i] });

                        logger.Information(
                            $"Mensagem: {args[i]} | " +
                            $"Status: { result.Status.ToString()}");
                    }
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