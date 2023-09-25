using Confluent.Kafka;
using System.Security.Cryptography;
using System.Text;

var config = new ProducerConfig { 
    BootstrapServers = "192.168.18.128",
};
var key = "samplekey1234567";

using var producer = new ProducerBuilder<Null, string>(config).Build();
TopicPartition topicPartition = new TopicPartition("sampleTopic1010", new Partition(1));
Console.WriteLine("Enter the input pls :   ");
bool exit = true;
while (exit)
{
    string value = Console.ReadLine();
    if (value == "q")
    {
        exit = false;
    }
    else
    {
        string encryptedMessage = Encrypt(value, key);
        await producer.ProduceAsync(topicPartition, new Message<Null, string> { Value = encryptedMessage });
        producer.Flush(TimeSpan.FromSeconds(10));

    }
}
static string Encrypt(string message, string key)
{
    using (Aes aes = Aes.Create())
    {
        aes.Key = Encoding.UTF8.GetBytes(key);
        aes.IV = new byte[16];

        ICryptoTransform encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
        byte[] encryptedBytes = encryptor.TransformFinalBlock(Encoding.UTF8.GetBytes(message), 0, message.Length);
        return Convert.ToBase64String(encryptedBytes);
    }
}
Console.ReadLine();