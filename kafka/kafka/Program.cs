using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System;
using System.Security.Cryptography;
using System.Text;

var consumerConfig = new ConsumerConfig

{

    BootstrapServers = "192.168.18.128",

    GroupId = "group2",

    AutoOffsetReset = AutoOffsetReset.Latest

};
var key = "samplekey1234567";
using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
TopicPartitionOffset topicPartitionOffset = new TopicPartitionOffset("sampleTopic1010", new Partition(1), Offset.Beginning);
consumer.Assign(new List<TopicPartitionOffset> { topicPartitionOffset });
try

{

    while (true)

    {

        var result = consumer.Consume(TimeSpan.FromSeconds(1));
        if (result != null)
        {
            string decryptedMessage = Decrypt(result.Message.Value, key);
            Console.WriteLine($"Consumed message '{decryptedMessage}' at: '{result.TopicPartitionOffset}'.");

        }

        if (result == null)

        {

            continue;

        }

    }

}

catch (Exception ex)

{

    Console.WriteLine($"Error: {ex.Message}");

}

static string Decrypt(string encryptedMessage, string key)
{
    using (Aes aes = Aes.Create())
    {
        aes.Key = Encoding.UTF8.GetBytes(key);
        aes.IV = new byte[16];

        ICryptoTransform decryptor = aes.CreateDecryptor(aes.Key, aes.IV);
        byte[] encryptedBytes = Convert.FromBase64String(encryptedMessage);
        byte[] decryptedBytes = decryptor.TransformFinalBlock(encryptedBytes, 0, encryptedBytes.Length);
        return Encoding.UTF8.GetString(decryptedBytes);
    }
}
