using Azure.Core;
using Azure.Identity;
using Databricks.Sql.Net.Options;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Authentication
{
    /// <summary>
    /// Custom Authentication, relying on TokenCredential.
    /// Usually using a DefautAzureCredential
    /// </summary>
    /// <param name="tokenCredential"></param>
    /// <param name="options"></param>
    public class AuthenticationProvider(TokenCredential tokenCredential, SqlWarehouseOptions options) 
    {
        private readonly SqlWarehouseOptions options = options;
        private readonly TokenCredential credential = tokenCredential;

        public TokenCredential Credential => this.credential;

        public async Task<string> GetTokenAsync(CancellationToken cancellationToken = default)
        {
            // create a token request context for the databricks resource on a specific tenant id.
            var tokenRequest = new TokenRequestContext([$"{Constants.DatabricksResourceId}/.default"], tenantId: options.TenantId);

            // get the token
            var accessToken = await Credential.GetTokenAsync(tokenRequest, cancellationToken).ConfigureAwait(false);

            // return the token
            return accessToken.Token;
        }
    }
}
