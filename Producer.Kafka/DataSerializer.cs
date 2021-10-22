using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Producer.Kafka
{
    public class DataSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
        }
    }
}