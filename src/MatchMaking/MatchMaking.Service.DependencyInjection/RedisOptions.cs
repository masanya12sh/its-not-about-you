namespace MatchMaking.Service.DependencyInjection;

public class RedisOptions
{
    public const string SectionName = "RedisOptions";
    public string ConnectionString { get; set; }
}
