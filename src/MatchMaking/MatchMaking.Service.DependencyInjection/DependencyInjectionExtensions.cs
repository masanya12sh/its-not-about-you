using Confluent.Kafka;
using MatchMaking.Kafka.Serializers;
using MatchMaking.Service.Abstractions;
using MatchMaking.Service.Dto;
using MatchMaking.Service.Services;
using MatchMaking.Service.WebApi;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MatchMaking.Service.DependencyInjection;

//In normal project I would keep DI project separate and referenced only by top level DI from entrypoint
//of course i woul add config verification etc...but it's out of scope now
//also the good way is to inject IOptions...
public static class DependencyInjectionExtensions
{
    public static IServiceCollection AddMatchMaking(this IServiceCollection services)
    {
        return services
            .AddRedis()
            .AddKafka()
            .AddAppControllers()
            .AddHostedService<MatchCompleteConsumer>();
    }

    public static IServiceCollection AddAppControllers(this IServiceCollection services)
    {
        services.AddRateLimiter(RateLimiterHelper.Handle)
            .AddControllers(options => options.Conventions.Add(new ApiRoutePrefixConvention()))
            .AddApplicationPart(typeof(MatchInfoController).Assembly)
            .AddControllersAsServices();
        return services;
    }

    public static IServiceCollection AddKafka(this IServiceCollection services)
    {
        return services
            .AddSingleton(serviceProvider =>
            {
                var producerConfig = serviceProvider.GetRequiredService<IOptions<ProducerConfig>>().Value;
                return new ProducerBuilder<Null, MatchSearchRequestMessage>(producerConfig)
                .SetValueSerializer(new KafkaJsonSerializer<MatchSearchRequestMessage>())
                .Build();
                //of course serializer also should be used / resolved via abstractions...
            })
            .AddSingleton(serviceProvider =>
            {
                var consumerConfig = serviceProvider.GetRequiredService<IOptions<ConsumerConfig>>().Value;
                return new ConsumerBuilder<Null, MatchCompleteMessage>(consumerConfig)
                .SetValueDeserializer(new KafkaJsonDeserializer<MatchCompleteMessage>())
                .Build();
            });
    }

    public static IServiceCollection AddAppOptions(this IServiceCollection services,
        IConfiguration configuration)
    {
        return services
            .AddKafkaOptions(configuration.GetSection("Kafka"))
            .AddRedisOptions(configuration.GetSection(RedisOptions.SectionName));
    }

    public static IServiceCollection AddRedis(this IServiceCollection services)
    {
        return services
            .AddRedisCore()
            .AddSingleton<IRedisMatchStore, RedisMatchStore>();
    }

    public static IServiceCollection AddRedisCore(this IServiceCollection services)
    {
        return services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
        {
            var redisOptions = serviceProvider.GetRequiredService<IOptions<RedisOptions>>().Value;
            return ConnectionMultiplexer.Connect(redisOptions.ConnectionString);
        });
    }

    public static IServiceCollection AddKafkaOptions(this IServiceCollection services,
        IConfigurationSection configurationSection)
    {
        return services
            .Configure<ProducerConfig>(configurationSection)
            .Configure<ConsumerConfig>(configurationSection);
    }

    public static IServiceCollection AddRedisOptions(this IServiceCollection services,
        IConfigurationSection configurationSection)
    {
        return services.Configure<RedisOptions>(configurationSection);
    }
}
