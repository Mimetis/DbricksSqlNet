using Azure.Core;
using Databricks.Sql.Net.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Authentication
{

    /// <summary>
    /// Token Credential for ApiKey
    /// </summary>
    /// <param name="apiKey"></param>
    public class ApiKeyTokenCredential : TokenCredential
    {
        public ApiKeyTokenCredential(SqlWarehouseOptions options)
        {
            if (string.IsNullOrEmpty(options.ApiKey))
                throw new InvalidOperationException("ApiKey is required");

            this.ApiKey = options.ApiKey;
        }

        public string ApiKey { get; }

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) => new(ApiKey, DateTime.Now.AddDays(1000));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) => new(new AccessToken(ApiKey, DateTime.Now.AddDays(1000)));
    }
}
