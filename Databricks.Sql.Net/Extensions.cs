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
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
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

        public static void Add(this List<SqlWarehouseParameter> list, string parameterName, SqlWarehouseType type, object value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = type, Value = value });
        public static void AddString(this List<SqlWarehouseParameter> list, string parameterName, string value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.STRING, Value = value });
        public static void AddTinyint(this List<SqlWarehouseParameter> list, string parameterName, byte value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.TINYINT, Value = value });
        //public static void AddBytes(this List<SqlWarehouseParameter> list, string parameterName, byte[] value)
        //    => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.BINARY, Value = value });
        public static void AddBoolean(this List<SqlWarehouseParameter> list, string parameterName, bool value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.BOOLEAN, Value = value });
        public static void AddDate(this List<SqlWarehouseParameter> list, string parameterName, DateOnly value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.DATE, Value = value });
        public static void AddBigInt(this List<SqlWarehouseParameter> list, string parameterName, long value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.BIGINT, Value = value });
        public static void AddDecimal(this List<SqlWarehouseParameter> list, string parameterName, decimal value, byte? precision = null, byte? scale = null )
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.DECIMAL, Value = value, Scale = scale, Precision = precision });
        public static void AddDouble(this List<SqlWarehouseParameter> list, string parameterName, double value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.DOUBLE, Value = value });
        public static void AddFloat(this List<SqlWarehouseParameter> list, string parameterName, float value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.FLOAT, Value = value });
        public static void AddInt(this List<SqlWarehouseParameter> list, string parameterName, int value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.INT, Value = value });
        public static void AddSmallInt(this List<SqlWarehouseParameter> list, string parameterName, short value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.SMALLINT, Value = value });
        public static void AddTimestamp(this List<SqlWarehouseParameter> list, string parameterName, DateTime value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.TIMESTAMP, Value = value });
        public static void AddTimestampTz(this List<SqlWarehouseParameter> list, string parameterName, DateTime value)
            => list.Add(new SqlWarehouseParameter { ParameterName = parameterName, Type = SqlWarehouseType.TIMESTAMP_NTZ, Value = value });



    }
}
