using MatchMaking.Service.Abstractions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace MatchMaking.Service.WebApi;

[Route("match")]
public class MatchInfoController : ControllerBase
{
    private readonly IRedisMatchStore _store;
    private readonly ILogger<MatchInfoController> _logger;

    public MatchInfoController(IRedisMatchStore store,
        ILogger<MatchInfoController> logger)
    {
        _store = store;
        _logger = logger;
    }

    [HttpGet("{userId}")]
    public async Task<IActionResult> SearchAsync([FromRoute] string userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
        {
            return BadRequest("userId is required.");
        }
        //move to BL out of transport
        var matchInfo = await _store.GetMatchAsync(userId);

        if (matchInfo is null)
        {
            _logger.LogInformation("Match not found for userId: {UserId}", userId);
            return NotFound();
        }

        _logger.LogInformation("Match retrieved for userId: {UserId}, MatchId: {MatchId}", userId, matchInfo.MatchId);
        return Ok(matchInfo);
    }
}
