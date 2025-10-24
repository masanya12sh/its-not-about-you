using MatchMaking.Service.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddAppOptions(builder.Configuration)
    .AddMatchMaking();

var app = builder.Build();

app.MapControllers();
app.UseRateLimiter();

app.Run();
