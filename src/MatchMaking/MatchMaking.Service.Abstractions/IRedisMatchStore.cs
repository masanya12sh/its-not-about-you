using MatchMaking.Service.WebApi.Dto;

namespace MatchMaking.Service.Abstractions;

public interface IRedisMatchStore
{
    Task StoreMatchAsync(MatchInformationResponse matchInfo);
    Task<MatchInformationResponse> GetMatchAsync(string userId);
}