namespace MatchMaking.Service.WebApi.Dto;

public record MatchInformationResponse(
    Guid MatchId,
    List<string> UserIds
);
