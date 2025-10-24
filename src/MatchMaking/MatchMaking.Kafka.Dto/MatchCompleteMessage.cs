namespace MatchMaking.Service.Dto;

public record MatchCompleteMessage(
    Guid MatchId,
    List<string> UserIds
);
