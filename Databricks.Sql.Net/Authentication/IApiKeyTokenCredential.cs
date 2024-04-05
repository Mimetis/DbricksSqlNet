using Azure.Core;
using System.Threading;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Authentication
{
    public interface IApiKeyTokenCredential
    {
        string ApiKey { get; }

        AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken);
        ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken);
    }
}