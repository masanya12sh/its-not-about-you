using System.Text.Json;
using Confluent.Kafka;

namespace MatchMaking.Kafka.Serializers;

public class KafkaJsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
        => JsonSerializer.SerializeToUtf8Bytes(data);
}