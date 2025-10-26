using Confluent.Kafka;
using MatchMaking.Kafka.Dto;
using MatchMaking.Service.Dto;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace MatchMaking.Service.WebApi;

[Route("match")]
public class MatchSearchController : ControllerBase
{
    private readonly IProducer<Null, MatchSearchRequestMessage> _producer;
    private readonly ILogger<MatchSearchController> _logger;

    public MatchSearchController(IProducer<Null, MatchSearchRequestMessage> producer,
        ILogger<MatchSearchController> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    [HttpPost("{userId}")]
    public async Task<IActionResult> SearchAsync([FromRoute] string userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
        {
            _logger.LogError("Search request failed: Invalid userId.");
            return BadRequest("userId is required.");
        }

        try
        {
            //not good at all to do it in transport layer...since it's BL
            await _producer.ProduceAsync(KafkaConstants.RequestTopic,
                new() { Value = new(userId) });
            _logger.LogInformation("Match search requested for userId: {UserId}", userId);
            return NoContent();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to produce message to Kafka for user {UserId}", userId);
            return StatusCode(StatusCodes.Status500InternalServerError);
        }
    }
}
