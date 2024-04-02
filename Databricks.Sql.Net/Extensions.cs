using DataBrickConnector.Models;
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
        ///     }
        /// </code>
        /// </example>
        /// </summary>
        public static IServiceCollection AddDataBricksSqlNet(
          this IServiceCollection services,
          IConfiguration configuration,
          string configSectionName = "Databricks")
        {
            ArgumentNullException.ThrowIfNull(configuration);

            try
            {

                services.AddOptions<DbricksOptions>().Configure<IConfiguration>((options, configuration) =>
                    configuration.GetSection(configSectionName).Bind(options));

                services.AddTransient<DbricksConnection>();
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }

            return services;
        }


        public static JsonArray ToJsonArray(this DbricksResponse dbricksResponse)
        {
            if (dbricksResponse.Result.DataArray == null && dbricksResponse.Result.RowCount == 0)
            {
                return [];
            }
            else
            {
                return new(
                    dbricksResponse.Result.DataArray
                        .Select(row =>
                            new JsonObject(
                                dbricksResponse.Manifest.Schema.Columns
                                    .Select(column => new KeyValuePair<string, JsonNode?>(column.Name,
                                            GetJsonNode(row[column.Position], column.TypeName)))
                                )
                        )
                        .ToArray()
                );
            }
        }

        private static JsonNode? GetJsonNode(string valueStr, string columnTypeName)
        {
            try
            {
                return columnTypeName switch
                {
                    "INT" => string.IsNullOrEmpty(valueStr) ? default : int.Parse(valueStr),
                    "LONG" => string.IsNullOrEmpty(valueStr) ? default : long.Parse(valueStr),
                    "DOUBLE" => string.IsNullOrEmpty(valueStr) ? default : double.Parse(valueStr, System.Globalization.CultureInfo.InvariantCulture),
                    "DECIMAL" => string.IsNullOrEmpty(valueStr) ? default : double.Parse(valueStr, System.Globalization.CultureInfo.InvariantCulture),
                    "TIMESTAMP" => string.IsNullOrEmpty(valueStr) ? default : DateTime.Parse(valueStr, System.Globalization.CultureInfo.InvariantCulture),
                    "DATE" => string.IsNullOrEmpty(valueStr) ? default : DateTime.Parse(valueStr, System.Globalization.CultureInfo.InvariantCulture),
                    "BOOLEAN" => string.IsNullOrEmpty(valueStr) ? default : bool.Parse(valueStr),
                    _ => string.IsNullOrEmpty(valueStr) ? string.Empty : valueStr,
                };
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error parsing value {valueStr} to type {columnTypeName}; {ex}");
                throw;
            }
        }

    }
}
