using Azure.Core;
using Azure.Identity;
using Databricks.Sql.Net.Authentication;
using Databricks.Sql.Net.Options;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Client
{
    public class SqlWarehouseConnection
    {

        /// <summary>
        /// Gets or Sets custom headers to add to each request
        /// </summary>
        public Dictionary<string, string> CustomHeaders = [];


        /// <summary>
        /// Gets or Sets the query parameters to add to each request
        /// </summary>
        public Dictionary<string, string> QueryParameters = [];


        public HttpClient HttpClient { get; }
        public Policy Policy { get; }


        public SqlWarehouseOptions Options { get; }

        /// <summary>
        /// Gets the root uri of the databricks instance, based on option name Host
        /// </summary>
        public Uri RootUri { get; }

        /// <summary>
        /// Gets or Sets the authentication method
        /// </summary>
        public AuthenticationProvider Authentication { get; set; }


        public Uri GetSqlStatementsPath() => GetPath(Constants.SqlStatementsPath);

        public Uri GetSqlStatementsResultPath(string statementId) => GetPathWithQueryParmaters(Constants.SqlStatementsResultPath, statementId);

        public SqlWarehouseConnection(IOptions<SqlWarehouseOptions> dbriksOptions, HttpClient client = null, Policy policy = null, TokenCredential customTokenCredential = null)
            : this(dbriksOptions.Value, client, policy, customTokenCredential)
        {
        }

        public SqlWarehouseConnection(SqlWarehouseOptions dbriksOptions, HttpClient client = null, Policy policy = null, TokenCredential customTokenCredential = null)
        {
            Policy = EnsurePolicy(policy);
            Options = dbriksOptions;
            RootUri = new Uri(Options.Host);

            // if no HttpClient provisionned, create a new one
            if (client == null)
            {
                var handler = new HttpClientHandler();

                // Activated by default
                if (handler.SupportsAutomaticDecompression)
                    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

                HttpClient = new HttpClient(handler);
            }
            else
            {
                HttpClient = client;
            }

            // create a chained token credential based on the options
            var azureAuthOptions = new DefaultAzureCredentialOptions
            {
                ExcludeInteractiveBrowserCredential = false,
                ManagedIdentityClientId = this.Options.ManagedIdentityClientId,
                TenantId = this.Options.TenantId,
            };
            var credential = new ChainedTokenCredential(new ApiKeyTokenCredential(Options), new DefaultAzureCredential(azureAuthOptions));

            Authentication = new AuthenticationProvider(credential, dbriksOptions);
        }

        private Uri GetPath(string relativePath) => new(RootUri, relativePath);

        private Uri GetPathWithQueryParmaters(string relativePath, params object[] queryParameters) => GetPath(string.Format(relativePath, queryParameters));


        private static Policy EnsurePolicy(Policy policy)
        {
            if (policy != default)
                return policy;

            // Defining my retry policy
            policy = Policy.WaitAndRetry(1, TimeSpan.FromMilliseconds(500));

            return policy;
        }
    }
}
