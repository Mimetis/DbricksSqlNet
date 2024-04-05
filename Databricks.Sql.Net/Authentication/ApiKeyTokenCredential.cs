using System;
using Azure.Core;
using Databricks.Sql.Net.Options;
using Azure.Core.Pipeline;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;


namespace Databricks.Sql.Net.Authentication
{

    /// <summary>
    /// Token Credential for ApiKey
    /// </summary>
    public class ApiKeyTokenCredential(SqlWarehouseOptions options) : TokenCredential, IApiKeyTokenCredential
    {
        public string ApiKey { get; } = options.ApiKey;

        /// <summary>
        /// Get the token from the ApiKey stored in appSettings
        /// </summary>
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            var valueTask = GetTokenAsync(requestContext, cancellationToken);
            if (valueTask.IsCompleted)
                return valueTask.Result;

            throw new InvalidOperationException("GetToken Task is not completed");
        }

        /// <summary>
        /// Get the token from the ApiKey stored in appSettings
        /// </summary>
        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(ApiKey))
                throw new CredentialUnavailableException("ApiKey is required");

            return new(new AccessToken(ApiKey, DateTime.Now.AddDays(1000)));
        }
    }
}
