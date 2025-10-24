using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;

namespace MatchMaking.Service.WebApi;

//shoudn't be like that in prod, but to keep di clear...
public static class RateLimiterHelper
{
    public static void Handle(RateLimiterOptions options)
    {
        //I would count the rate by userId or IP, but you haven't specified it, so...
        options.AddFixedWindowLimiter(policyName: "static-rate-limiter", opt =>
        {
            opt.PermitLimit = 1;
            opt.Window = TimeSpan.FromMilliseconds(100);
            //Queueing might be added
            //opt.QueueLimit = 100;
            //opt.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
        });

        //returning 400 as you have mention to use 204 & 400 only, but I would use 429
        options.RejectionStatusCode = StatusCodes.Status400BadRequest;
    }
}
