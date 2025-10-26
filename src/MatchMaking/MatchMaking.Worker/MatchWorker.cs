using Confluent.Kafka;
using MatchMaking.Kafka.Dto;
using MatchMaking.Service.Dto;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MatchMaking.Worker;

public class MatchWorker : BackgroundService
{
    public const string QueueKey = "matchmaking:queue";
    public const string MatchWortkerStartedKey = @"MatchWorker started, requiring {Count}
 players per match. Listening to topic '{Topic}'.";
    public const string UserAddedKey = "User {UserId} added to queue. Current size: {Length}.";
    public const string MatchWorkerConsumptionCamcelledKey = @"MatchWorker consumption was
 cancelled due to host shutdown.";

    private readonly ILogger<MatchWorker> _logger;
    private readonly IOptions<MatchMakingOptions> _options;
    private readonly IDatabase _redisDb;
    private readonly IConsumer<string, MatchSearchRequestMessage> _requestConsumer;
    private readonly IProducer<Null, MatchCompleteMessage> _completionProducer;

    public MatchWorker(
        ILogger<MatchWorker> logger,
        IOptions<MatchMakingOptions> config,
        IConnectionMultiplexer redis,
        IConsumer<string, MatchSearchRequestMessage> requestConsumer,
        IProducer<Null, MatchCompleteMessage> completionProducer)
    {
        _logger = logger;
        _options = config;
        _redisDb = redis.GetDatabase();
        _requestConsumer = requestConsumer;
        _completionProducer = completionProducer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        //i'm sure we can improve overall logic but i guess it's kindof not what U wanna see
        _logger.LogInformation(MatchWortkerStartedKey,
            _options.Value.RequiredPlayersPerMatch, KafkaConstants.RequestTopic);

        _requestConsumer.Subscribe(KafkaConstants.RequestTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = _requestConsumer.Consume(stoppingToken);

                var request = consumeResult.Message.Value;
                var userId = request.UserId;

                var newQueueLength = await _redisDb.ListRightPushAsync(QueueKey, userId);
                _logger.LogDebug(UserAddedKey, userId, newQueueLength);

                await CheckAndFormMatchAsync(newQueueLength, stoppingToken);

                _requestConsumer.Commit(consumeResult);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(MatchWorkerConsumptionCamcelledKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An unexpected error occurred during Kafka consumption in the MatchWorker.");
        }
        finally
        {
            _requestConsumer.Close();
        }
    }

    private async Task CheckAndFormMatchAsync(long currentQueueLength,
        CancellationToken cancellationToken)
    {
        //i'm sure we can improve overall logic but i guess it's kindof not what U wanna see
        while (currentQueueLength >= _options.Value.RequiredPlayersPerMatch)
        {
            _logger.LogInformation("Queue size ({Current}) sufficient for a match of {Required} players. Attempting to form match.",
                currentQueueLength, _options.Value.RequiredPlayersPerMatch);

            var requiredPlayers = _options.Value.RequiredPlayersPerMatch;
            var matchedUserIds = new List<string>();

            for (int i = 0; i < requiredPlayers; i++)
            {
                var userValue = await _redisDb.ListLeftPopAsync(QueueKey);

                if (userValue.HasValue)
                {
                    matchedUserIds.Add(userValue.ToString());
                }
                else
                {
                    _logger.LogWarning("Concurrent access detected. Failed to extract enough users. Only got {Count}.", matchedUserIds.Count);

                    foreach (var id in matchedUserIds)
                    {
                        await _redisDb.ListLeftPushAsync(QueueKey, id);
                    }
                    return;
                }
            }

            var matchId = Guid.NewGuid();
            var completeMessage = new MatchCompleteMessage(matchId, matchedUserIds);

            await _completionProducer.ProduceAsync(KafkaConstants.CompleteTopic, new Message<Null, MatchCompleteMessage>
            {
                Value = completeMessage
            }, cancellationToken);

            _logger.LogInformation(
                "Match successfully completed. MatchId: {MatchId}, Users: {UserList}",
                matchId,
                string.Join(", ", matchedUserIds)
            );

            currentQueueLength -= requiredPlayers;
        }
    }
}
