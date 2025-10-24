using System.Text.Json;
using Confluent.Kafka;

namespace MatchMaking.Kafka.Serializers;

public class KafkaJsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull) return default;
        return JsonSerializer.Deserialize<T>(data);
    }
}