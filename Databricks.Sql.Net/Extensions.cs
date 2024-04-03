using Azure.Core;
using Azure.Identity;
using Databricks.Sql.Net.Authentication;
using Databricks.Sql.Net.Client;
using Databricks.Sql.Net.Models;
using Databricks.Sql.Net.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Databricks.Sql.Net
{
    public static class Extensions
    {
        /// <summary>
        /// Add the Databricks Options. This will bind the configuration section to the DbricksOptions class
        /// Section should be named "Databricks"
        /// Section should have the following keys: Host, ApiKey, WarehouseId, Catalog, Schema
        /// <example>
        /// <code>
        ///    "Databricks": {
        ///         "Host": "https://adb-xxxxxxx.azuredatabricks.net",
        ///         "ApiKey": "dapi-xxxxx",
        ///         "WarehouseId": "xxxxxxx",
        ///         "Catalog": "hive_catalog",
        ///         "Schema": "schema_default"
        ///         "WaitTimeout": 30,
        ///         "ManagedIdentityClientId": "xxxxx-xxxx-xxxx-xxxx",
        ///         "TenantId": "xxxxx-xxxx-xxxx-xxxx"
        ///     }
        /// </code>
        /// </example>
        /// </summary>
        private static IServiceCollection AddSqlWarehouse(
          this IServiceCollection services,
          IConfiguration configuration,
          string configSectionName,
          Policy policy,
          DefaultAzureCredentialOptions defaultAzureCredentialOptions, TokenCredential customTokenCredential)
        {
            ArgumentNullException.ThrowIfNull(configuration);

            try
            {
                services.AddHttpClient<SqlWarehouseConnection>();
                services.AddOptions<SqlWarehouseOptions>().Configure<IConfiguration>((options, configuration) =>
                    configuration.GetSection(configSectionName).Bind(options));

                services.AddTransient((s) =>
                {
                    var options = s.GetRequiredService<IOptions<SqlWarehouseOptions>>().Value;
                    var httpClient = s.GetRequiredService<IHttpClientFactory>().CreateClient();
                    var connection = new SqlWarehouseConnection(options, httpClient, policy, defaultAzureCredentialOptions, customTokenCredential);
                    return connection;
                });
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }

            return services;
        }

        /// <inheritdoc cref="AddSqlWarehouse(IServiceCollection, IConfiguration, string, Policy, DefaultAzureCredentialOptions, TokenCredential)"/>/>
        public static IServiceCollection AddSqlWarehouse(this IServiceCollection services, IConfiguration configuration,
            string configSectionName = "Databricks", Policy policy = default)
        {
            return AddSqlWarehouse(services, configuration, configSectionName, policy, default, default);
        }

        /// <inheritdoc cref="AddSqlWarehouse(IServiceCollection, IConfiguration, string, Policy, DefaultAzureCredentialOptions, TokenCredential)"/>/>
        public static IServiceCollection AddSqlWarehouse(this IServiceCollection services, IConfiguration configuration,
            DefaultAzureCredentialOptions defaultAzureCredentialOptions, string configSectionName = "Databricks", Policy policy = default)
        {
            return AddSqlWarehouse(services, configuration, configSectionName, policy, defaultAzureCredentialOptions, default);
        }

        /// <inheritdoc cref="AddSqlWarehouse(IServiceCollection, IConfiguration, string, Policy, DefaultAzureCredentialOptions, TokenCredential)"/>/>
        public static IServiceCollection AddSqlWarehouse(this IServiceCollection services, IConfiguration configuration,
            TokenCredential customTokenCredential, string configSectionName = "Databricks", Policy policy = default)
        {
            return AddSqlWarehouse(services, configuration, configSectionName, policy, default, customTokenCredential);
        }

    }
}
