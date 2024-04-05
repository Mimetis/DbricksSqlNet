using Azure.Core;
using Azure.Identity;
using Databricks.Sql.Net.Authentication;
using Databricks.Sql.Net.Models.Sql;
using Databricks.Sql.Net.Options;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
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


        /// <summary>
        /// Gets the HttpClient used to make the requests
        /// </summary>
        public HttpClient HttpClient { get; }

        /// <summary>
        /// Gets or Sets the policy to apply to the requests
        /// </summary>
        public Policy Policy { get; set; }

        /// <summary>
        /// Gets the options used to configure the databricks connection
        /// </summary>
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

        public SqlWarehouseConnection(SqlWarehouseOptions dbriksOptions, HttpClient client = null, Policy policy = null,
            DefaultAzureCredentialOptions defaultAzureCredentialOptions = null, TokenCredential customTokenCredential = null)
        {
            this.Policy = EnsurePolicy(policy);
            this.Options = dbriksOptions;
            this.RootUri = new Uri(Options.Host);

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
            var azureAuthOptions = defaultAzureCredentialOptions ?? new DefaultAzureCredentialOptions
            {
                ExcludeInteractiveBrowserCredential = false,
                ManagedIdentityClientId = this.Options.ManagedIdentityClientId,
                TenantId = this.Options.TenantId,
            };

            var credential = customTokenCredential ?? new ChainedTokenCredential(new ApiKeyTokenCredential(Options), new DefaultAzureCredential(azureAuthOptions));

            this.Authentication = new AuthenticationProvider(credential, dbriksOptions);
        }


        /// <summary>
        /// Get the Sql Warehouses data sources
        /// </summary>
        public async Task<List<Warehouse>> GetSqlWarehousesAsync(CancellationToken cancellationToken = default)
        {
            var uri = GetPath(Constants.SqlWarehousesList);
            var token = await this.Authentication.GetTokenAsync(cancellationToken);
            var result = await Policy.ExecuteAsync(() => this.SendAsync<List<Warehouse>>(uri, HttpMethod.Get, token, null, cancellationToken), cancellationToken).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Gets the Sql Warehouse Queries
        /// </summary>
        public async Task<Query> GetSqlWarehousesQueriesAsync(CancellationToken cancellationToken = default)
        {
            var uri = GetPath(Constants.SqlQueriesList);
            var token = await this.Authentication.GetTokenAsync(cancellationToken);
            var result = await Policy.ExecuteAsync(() => this.SendAsync<Query>(uri, HttpMethod.Get, token, null, cancellationToken), cancellationToken).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Gets the Sql Warehouse Queries
        /// </summary>
        public async Task<QueryHistory> GetSqlWarehousesQueriesHystoryAsync(int? maxResults = default, string pageToken = default, bool includeMetrics = false, CancellationToken cancellationToken = default)
        {
            var uri = GetPath(Constants.SqlQueriesHistory).ToString();
            var token = await this.Authentication.GetTokenAsync(cancellationToken);

            if (maxResults.HasValue)
                uri = QueryHelpers.AddQueryString(uri, "max_results", maxResults.Value.ToString());

            if (!string.IsNullOrEmpty(pageToken))
                uri = QueryHelpers.AddQueryString(uri, "page_token", pageToken);

            if (includeMetrics)
                uri = QueryHelpers.AddQueryString(uri, "include_metrics", "true");

            var result = await Policy.ExecuteAsync(() => this.SendAsync<QueryHistory>(new Uri(uri), HttpMethod.Get, token, null, cancellationToken), cancellationToken).ConfigureAwait(false);
            return result;
        }


        /// <summary>
        /// Gets the Sql Warehouse Queries
        /// </summary>
        public async Task<WarehousePermissions> GetSqlWarehousePermissionsAsync(string warehouseId, CancellationToken cancellationToken = default)
        {
            var uri = GetPath(string.Format(Constants.SqlWarehousePermissions, warehouseId));
            var token = await this.Authentication.GetTokenAsync(cancellationToken);

            var result = await Policy.ExecuteAsync(() => this.SendAsync<WarehousePermissions>(uri, HttpMethod.Get, token, null, cancellationToken), cancellationToken).ConfigureAwait(false);
            return result;

        }


        /// <summary>
        /// Get current schema
        /// </summary>
        /// <returns></returns>
        public async Task<Schema> GetSchemaAsync()
        {
            var commandSchema = new SqlWarehouseCommand(this, $"DESCRIBE SCHEMA {this.Options.Schema};");
            var statement = await commandSchema.ExecuteAsync();

            var schema = new Schema();

            if (statement.Result != null)
            {
                if (statement.Result.RowCount > 0)
                    schema.Catalog = statement.Result.DataArray[0][1];

                if (statement.Result.RowCount > 1)
                    schema.Name = statement.Result.DataArray[1][1];

                if (statement.Result.RowCount > 2)
                    schema.Comment = statement.Result.DataArray[2][1];

                if (statement.Result.RowCount > 3)
                    schema.Location = statement.Result.DataArray[3][1];
            }

            var commandTables = new SqlWarehouseCommand(this, $"SHOW TABLES FROM {this.Options.Schema};");
            var stTables = await commandTables.ExecuteAsync();

            if (stTables.Result != null)
            {
                schema.Tables = [];

                foreach (var row in stTables.Result.DataArray)
                {
                    var table = new Table
                    {
                        Name = row[1],
                        IsTemporary = TypeConverter.TryConvertTo<bool>(row[2]),
                        Catalog = this.Options.Catalog,
                        Schema = this.Options.Schema
                    };
                    schema.Tables.Add(table);
                }
            }


            return schema;
        }

        /// <summary>
        /// Get current schema
        /// </summary>
        /// <returns></returns>
        public async Task<Table> GetTableAsync(string tableName)
        {
            var commandSchema = new SqlWarehouseCommand(this, $"DESCRIBE TABLE EXTENDED {tableName};");
            var statement = await commandSchema.ExecuteAsync();

            var table = new Table();

            if (statement.Result != null && statement.Result.DataArray != null && statement.Result.DataArray.Length > 0)
            {
                table.Columns = [];
                table.Properties = [];

                var isColumnDefinition = true;
                foreach (var row in statement.Result.DataArray)
                {
                    var col1 = row[0];
                    var col2 = row[1];

                    if (string.IsNullOrEmpty(col1) || col1.StartsWith('#'))
                    {
                        isColumnDefinition = false;
                        continue;
                    }

                    if (isColumnDefinition)
                    {
                        table.Columns.Add(new() { Name = col1, DataType = col2 });
                        continue;
                    }

                    if (col1 == "Catalog")
                        table.Catalog = col2;
                    else if (col1 == "Database")
                        table.Schema = col2;
                    else if (col1 == "Table")
                        table.Name = col2;
                    else
                        table.Properties.Add(col1, col2);



                }

                foreach (var row in statement.Result.DataArray)
                {
                    var columnName = row[0];
                    var dataType = row[1];

                    if (string.IsNullOrEmpty(columnName) || columnName.StartsWith('#'))
                        break;

                    table.Columns.Add(new() { Name = columnName, DataType = dataType });
                }


            }

            return table;
        }



        public Uri GetPath(string relativePath) => new(this.RootUri, relativePath);

        private Uri GetPathWithQueryParmaters(string relativePath, params object[] queryParameters) => GetPath(string.Format(relativePath, queryParameters));

        private static Policy EnsurePolicy(Policy policy)
        {
            if (policy != default)
                return policy;

            // Defining my retry policy
            return Policy.WaitAndRetry(1, TimeSpan.FromMilliseconds(500));
        }


        internal async Task<T> SendAsync<T>(Uri requestUri, HttpMethod method, string token, byte[] binaryData = default, CancellationToken cancellationToken = default)
        {
            // Create the request message
            var requestMessage = new HttpRequestMessage(method, requestUri);

            // Add the Authorization header
            requestMessage.Headers.Add("Authorization", $"Bearer {token}");

            // Adding others headers
            if (this.CustomHeaders != null && this.CustomHeaders.Count > 0)
                foreach (var kvp in this.CustomHeaders)
                    if (!requestMessage.Headers.Contains(kvp.Key))
                        requestMessage.Headers.Add(kvp.Key, kvp.Value);

            // get byte array content
            if (method == HttpMethod.Post || method == HttpMethod.Put || method == HttpMethod.Patch || method == HttpMethod.Delete)
                requestMessage.Content = new ByteArrayContent(binaryData);

            // If Json, specify header
            if (requestMessage.Content != null && !requestMessage.Content.Headers.Contains("content-type"))
                requestMessage.Content.Headers.Add("content-type", "application/json");

            T res = default;

            // Eventually, send the request
            var response = await this.HttpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            // throw exception if response is not successfull
            // get response from server
            if (!response.IsSuccessStatusCode)
                await HandleSyncErrorAsync(response);


            var content = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            };


            res = JsonSerializer.Deserialize<T>(content, options);


            //using (var content = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false))
            //{
            //    if (content.CanRead)
            //        res = await JsonSerializer.DeserializeAsync<T>(content, cancellationToken: cancellationToken);
            //}

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            return res;
        }


        private static async Task HandleSyncErrorAsync(HttpResponseMessage response)
        {
            try
            {
                Exception exception = null;

                if (response.Content == null)
                    throw new Exception(response.ReasonPhrase);

                using var content = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                if (!content.CanRead)
                    throw new Exception(response.ReasonPhrase);

                var streamReader = new StreamReader(content);
                var errorString = await streamReader.ReadToEndAsync().ConfigureAwait(false);

                if (errorString != null)
                    exception = new Exception(errorString);
                else
                    exception = new Exception(response.ReasonPhrase);

                throw exception;

            }
            catch (Exception)
            {
                throw new Exception(response.ReasonPhrase);
            }


        }


    }
}
