using System.Text.Json;
using MatchMaking.Service.Abstractions;
using MatchMaking.Service.WebApi.Dto;
using StackExchange.Redis;

namespace MatchMaking.Service.Services;

public class RedisMatchStore : IRedisMatchStore
{
    public const string MatchKeyPrefix = "match:";

    private readonly IDatabase _redisDb;

    public RedisMatchStore(IConnectionMultiplexer redis)
    {
        _redisDb = redis.GetDatabase();
    }

    public async Task StoreMatchAsync(MatchInformationResponse matchInfo)
    {
        var json = JsonSerializer.Serialize(matchInfo);

        foreach (var userId in matchInfo.UserIds)
        {
            await _redisDb.StringSetAsync(MatchKeyPrefix + userId, json);
        }
    }

    public async Task<MatchInformationResponse> GetMatchAsync(string userId)
    {
        var json = await _redisDb.StringGetAsync(MatchKeyPrefix + userId);

        if (json.IsNull)
        {
            return null;
        }

        return JsonSerializer.Deserialize<MatchInformationResponse>(json);
    }
}