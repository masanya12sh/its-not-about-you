using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApplicationModels;

namespace MatchMaking.Service.WebApi;

public class ApiRoutePrefixConvention : IApplicationModelConvention
{
    private readonly AttributeRouteModel _routePrefix;

    public ApiRoutePrefixConvention()
    {
        _routePrefix = new AttributeRouteModel(new RouteAttribute("api"));
    }

    public void Apply(ApplicationModel application)
    {
        foreach (var controller in application.Controllers)
        {
            var matchedSelectors = controller.Selectors
                .Where(x => x.AttributeRouteModel != null)
                .ToList();
            if (matchedSelectors.Count != 0)
            {
                foreach (var selectorModel in matchedSelectors)
                {
                    selectorModel.AttributeRouteModel = AttributeRouteModel
                        .CombineAttributeRouteModel(_routePrefix, selectorModel.AttributeRouteModel);
                }
            }
        }
    }
}
