using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace EnvioKafka
{
    class Program
    {
        public static Random generate = new Random();

        static async Task Main(string[] args)
        {
            string bootstrapServers = "localhost:9092";
            string topicName = "topic-test";
            int numeroParticoes = 10;
            int qtdeRastreadores = 20;
            int qtdePacotes = 500;

            await CreateTopic(bootstrapServers, topicName, numeroParticoes);

            var rastreadores = new List<RastreadorEvent>();

            Random random = new Random();

            for (int i = 0; i < qtdeRastreadores; i++)
            {
                rastreadores.Add(new RastreadorEvent(random.Next(1111, 999999)));
            }
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                for (int i = 0; i < qtdePacotes; i++)
                {
                    Console.WriteLine($"Pacote: {i}/{qtdePacotes}");
                    int randomIndex = random.Next(0, rastreadores.Count);

                    var randomItem = rastreadores[randomIndex];
                    randomItem.Incrementa();
                    int lastDigit = (int)(randomItem.Rastreador % numeroParticoes);

                    await EnviarMensagem(producer, topicName, lastDigit, randomItem);
                }
            }
        }

        static async Task EnviarMensagem(IProducer<Null, string> producer, string topicName, int particao, RastreadorEvent evento)
        {
            try
            {
                var topicPart = new TopicPartition(topicName, new Partition(particao));
                var result = await producer.ProduceAsync(
                    topicPart,
                    new() { Value = JsonSerializer.Serialize(evento) });

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }

        static async Task CreateTopic(string bootstrapServers, string topicName, int numPartitions)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                    new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = numPartitions} });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }
    }
}