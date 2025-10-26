using Confluent.Kafka;
using MatchMaking.Kafka.Dto;
using MatchMaking.Service.Abstractions;
using MatchMaking.Service.Dto;
using MatchMaking.Service.WebApi.Dto;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MatchMaking.Service.Services;

public class MatchCompleteConsumer : IHostedService, IMatchCompleteConsumer
{
    private readonly ILogger<MatchCompleteConsumer> _logger;
    private readonly IRedisMatchStore _matchStore;
    private readonly IConsumer<Null, MatchCompleteMessage> _consumer;
    private readonly CancellationTokenSource _cancellationTokenSource;

    public MatchCompleteConsumer(
        ILogger<MatchCompleteConsumer> logger,
        IRedisMatchStore matchStore,
        IConsumer<Null, MatchCompleteMessage> consumer)
    {
        _logger = logger;
        _matchStore = matchStore;
        _consumer = consumer;
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public async Task ConsumeMatchCompleteAsync()
    {
        _consumer.Subscribe(KafkaConstants.CompleteTopic);

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(_cancellationTokenSource.Token);
                var message = consumeResult.Message.Value;

                var matchInfo = new MatchInformationResponse(message.MatchId, message.UserIds);
                await _matchStore.StoreMatchAsync(matchInfo);

                _logger.LogInformation("Consumed and stored completed match. MatchId: {MatchId}", message.MatchId);

                _consumer.Commit(consumeResult);
            }
            catch (ConsumeException e) when (e.Error.IsFatal)
            {
                _logger.LogError(e, "Fatal Kafka error.");
                break;
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        Task.Factory.StartNew(async (_) => await ConsumeMatchCompleteAsync(),
            _cancellationTokenSource.Token,
            TaskCreationOptions.LongRunning);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource.Cancel();
        _consumer.Close();
        return Task.CompletedTask;
    }
}
