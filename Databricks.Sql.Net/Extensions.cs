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
        ///         "UseManagedIdentity": true / false,
        ///         "TenantId": "xxxxx-xxxx-xxxx-xxxx"
        ///     }
        /// </code>
        /// </example>
        /// </summary>
        public static IServiceCollection AddSqlWarehouse(
          this IServiceCollection services,
          IConfiguration configuration,
          string configSectionName = "Databricks")
        {
            ArgumentNullException.ThrowIfNull(configuration);

            try
            {

                services.AddOptions<SqlWarehouseOptions>().Configure<IConfiguration>((options, configuration) =>
                    configuration.GetSection(configSectionName).Bind(options));

                services.AddTransient<SqlWarehouseConnection>();
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }

            return services;
        }
    }
}
