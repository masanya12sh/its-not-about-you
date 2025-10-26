namespace MatchMaking.Service.Abstractions;

public interface IMatchCompleteConsumer
{
    Task ConsumeMatchCompleteAsync();
}