//using Azure.Core;
//using Azure.Identity;
//using Databricks.Sql.Net.Options;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace Databricks.Sql.Net.Authentication
//{
//    public class ManagedIdentityAuthentication : IAuthentication
//    {
//        private readonly SqlWarehouseOptions options;

//        public TokenCredential Credential { get; }

//        public ManagedIdentityAuthentication(SqlWarehouseOptions options)
//        {
//            this.options = options;

//            if (!options.UseManagedIdentity)
//                throw new InvalidOperationException("Managed Identity is not enabled");

//            // user managed identity (otherwise use system managed identity)
//            if (this.options.ManagedIdentityClientId.HasValue)
//                Credential = new ManagedIdentityCredential(options.ManagedIdentityClientId.Value.ToString());
//            else
//                Credential = new ManagedIdentityCredential();

//        }

//        public async Task<string> GetTokenAsync(CancellationToken cancellationToken = default)
//        {
//            // create a token request context for the databricks resource on a specific tenant id.
//            var tokenRequest = new TokenRequestContext([$"{Constants.DatabricksResourceId}/.default"], tenantId: options.TenantId);

//            // get the token
//            var accessToken = await Credential.GetTokenAsync(tokenRequest, cancellationToken).ConfigureAwait(false);

//            // return the token
//            return accessToken.Token;
//        }
//    }
//}
