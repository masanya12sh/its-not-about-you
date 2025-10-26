using Confluent.Kafka;
using MatchMaking.Kafka.Serializers;
using MatchMaking.Service.Dto;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MatchMaking.Worker;

//In normal project I would keep DI project separate and referenced only by top level DI from entrypoint
//of course i woul add config verification etc...but it's out of scope now
//also the good way is to inject IOptions...
public static class DependencyInjectionExtensions
{
    public static IServiceCollection AddWorkerServices(this IServiceCollection services)
    {
        return services
            .AddRedis()
            .AddKafka()
            .AddHostedService<MatchWorker>();
    }

    public static IServiceCollection AddKafka(this IServiceCollection services)
    {
        return services
            .AddSingleton(serviceProvider =>
            {
                //of course serializer also should be used / resolved via abstractions...
                var consumerConfig = serviceProvider.GetRequiredService<IOptions<ConsumerConfig>>().Value;
                return new ConsumerBuilder<string, MatchSearchRequestMessage>(consumerConfig)
                    .SetValueDeserializer(new KafkaJsonDeserializer<MatchSearchRequestMessage>())
                    .Build();
            })
            .AddSingleton(serviceProvider =>
            {
                var producerConfig = serviceProvider.GetRequiredService<IOptions<ProducerConfig>>().Value;
                return new ProducerBuilder<Null, MatchCompleteMessage>(producerConfig)
                    .SetValueSerializer(new KafkaJsonSerializer<MatchCompleteMessage>())
                    .Build();
            });
    }

    public static IServiceCollection AddWorkerOptions(this IServiceCollection services,
        IConfiguration configuration)
    {
        return services
            .AddKafkaOptions(configuration.GetSection("Kafka"))
            .AddRedisOptions(configuration.GetSection(RedisOptions.SectionName))
            .AddMatchmakingOptions(configuration.GetSection(MatchMakingOptions.SectionName));
    }

    public static IServiceCollection AddRedisOptions(this IServiceCollection services,
        IConfigurationSection configurationSection)
    {
        return services.Configure<RedisOptions>(configurationSection);
    }

    public static IServiceCollection AddKafkaOptions(this IServiceCollection services,
        IConfigurationSection configurationSection)
    {
        return services
            .Configure<ProducerConfig>(configurationSection)
            .Configure<ConsumerConfig>(configurationSection);
    }

    public static IServiceCollection AddMatchmakingOptions(this IServiceCollection services,
        IConfiguration configurationSection)
    {
        services.AddOptions<MatchMakingOptions>()
            .Bind(configurationSection)
            .Validate(x => x.RequiredPlayersPerMatch > 0)
            .ValidateOnStart();
        return services;
    }

    public static IServiceCollection AddRedis(this IServiceCollection services)
    {
        return services.AddRedisCore();
    }

    public static IServiceCollection AddRedisCore(this IServiceCollection services)
    {
        return services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
        {
            var redisOptions = serviceProvider.GetRequiredService<IOptions<RedisOptions>>().Value;
            return ConnectionMultiplexer.Connect(redisOptions.ConnectionString);
        });
    }
}
