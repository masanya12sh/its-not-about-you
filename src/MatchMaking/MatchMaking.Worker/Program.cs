//PLEASE LOOK INTO SERVICE. It is done much better...

using MatchMaking.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddWorkerOptions(builder.Configuration)
    .AddWorkerServices();

var host = builder.Build();
host.Run();
