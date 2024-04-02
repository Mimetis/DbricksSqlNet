using DataBrickConnector.Models;
using Databricks.Sql.Net.Enumeration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Command
{
    public class DbricksCommand
    {

        /// <summary>
        /// Gets or Sets the command parameters.
        /// </summary>
        public List<DbricksCommandParameter> Parameters { get; set; } = [];

        /// <summary>
        /// Gets or sets the databricks connection.
        /// </summary>
        public DbricksConnection Connection { get; set; }

        /// <summary>
        /// Gets or Sets the command text to be executed.
        /// </summary>
        public string Command { get; set; }

        /// <summary>
        /// Gets or Sets the wait timeout in seconds.
        /// If null, gets the default value from the options parameters.
        /// </summary>
        public int? WaitTimeout { get; set; }

        public DbricksCommand()
        {

        }

        public DbricksCommand(DbricksConnection connetion, string command, int? waitTimeout = null)
        {
            this.Command = command;
            this.Connection = connetion;
            this.WaitTimeout = waitTimeout;
        }

        private byte[] BuildContent(int? limit)
        {

            var obj = new
            {
                warehouse_id = this.Connection.Options.WarehouseId,
                catalog = this.Connection.Options.Catalog,
                schema = this.Connection.Options.Schema,
                row_limit = limit ?? (int?)null,
                format = "JSON_ARRAY",
                disposition = "INLINE",
                wait_timeout = this.WaitTimeout.HasValue ? $"{this.WaitTimeout.Value}s" : $"{this.Connection.Options.WaitTimeout}s",
                statement = this.Command,
                parameters = this.Parameters == null || this.Parameters.Count <= 0 ? [] : this.Parameters.Select(p => new
                {
                    name = p.ParameterName,
                    type = p.TypeName,
                    value = p.Value

                }).ToArray(),

            };

            return JsonSerializer.SerializeToUtf8Bytes(obj);

        }

        public async Task<DbricksResponse> ExecuteAsync<T>(int? limit = default, IProgress<T> progress = default, CancellationToken cancellationToken = default)
        {
            if (this.Connection.HttpClient is null)
                throw new ArgumentNullException(nameof(this.Connection.HttpClient));

            if (this.Connection.Options.Host == null)
                throw new ArgumentException("Host is not defined");

            // Build the request uri to Sql Statements
            var requestUri = BuildUri(this.Connection.Options.Host, DbricksConnection.SqlStatementsPath);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // Execute my OpenAsync in my policy context
            var dbricksResponse = await this.Connection.Policy.ExecuteAsync(
                ct => this.SendAsync(requestUri, limit, ct), progress, cancellationToken);

            return dbricksResponse;
        }

        public async Task<T> LoadAsync<T>(int? limit = default, IProgress<T> progress = default, CancellationToken cancellationToken = default)
        {
            var dbricksResponse = await ExecuteAsync(limit, progress, cancellationToken);

            var jsonResult = dbricksResponse.ToJsonArray();
            var result = JsonSerializer.Deserialize<T>(jsonResult);

            return result;
        }

        public async Task<DataTable> LoadDataTableAsync(int? limit = default, IProgress<DataTable> progress = default, CancellationToken cancellationToken = default)
        {
            var dbricksResponse = await ExecuteAsync(limit, progress, cancellationToken);


            if (dbricksResponse == null)
                return default;

            // Build a datatable with columns based on dbricksResponse.Manifest.Schema.Columns columns
            var dt = new DataTable();

            foreach (var column in dbricksResponse.Manifest.Schema.Columns)
            {
                Type type = typeof(string);
                if (Enum.TryParse(column.TypeText, out DbricksType dbricksType))
                    type = GetTypeFromDbricksType(dbricksType);

                var dc = new DataColumn(column.Name, type);
                dt.Columns.Add(dc);
            }

            // Add rows to datatable using the dbricksResponse.Result.DataArray that is an array of array
            foreach (var row in dbricksResponse.Result.DataArray)
            {
                var dr = dt.NewRow();

                foreach (var column in dbricksResponse.Manifest.Schema.Columns)
                {
                    Type type = typeof(string);
                    if (Enum.TryParse(column.TypeText, out DbricksType dbricksType))
                        type = GetTypeFromDbricksType(dbricksType);


                    dr[column.Name] = TypeConverter.TryConvertTo(row[column.Position], type);
                }

                dt.Rows.Add(dr);
            }

            return dt;

        }

        public async Task<JsonArray> LoadJsonAsync(int? limit = default, IProgress<DataTable> progress = default, CancellationToken cancellationToken = default)
        {
            var dbricksResponse = await ExecuteAsync(limit, progress, cancellationToken);

            if (dbricksResponse == null)
                return default;

            // Build a datatable with columns based on dbricksResponse.Manifest.Schema.Columns columns
            var jsonArray = new JsonArray();
            foreach (var row in dbricksResponse.Result.DataArray)
            {
                var jsonRow = new JsonObject();
                foreach (var column in dbricksResponse.Manifest.Schema.Columns)
                {
                    Type type = typeof(string);

                    if (Enum.TryParse(column.TypeText, out DbricksType dbricksType))
                        type = GetTypeFromDbricksType(dbricksType);

                    var value = TypeConverter.TryConvertTo(row[column.Position], type);

                    jsonRow[column.Name] = JsonValue.Create(value);
                }

                jsonArray.Add(jsonRow);
            }

            return jsonArray;
        }

        private Type GetTypeFromDbricksType(DbricksType dbricksType)
        {
            // return dotnet type from enumeration DbricksType
            switch (dbricksType)
            {
                case DbricksType.BIGINT:
                    return typeof(Int64);
                case DbricksType.VOID:
                    return typeof(DBNull);
                case DbricksType.STRING:
                    return typeof(String);
                case DbricksType.DATE:
                    return typeof(DateOnly);
                case DbricksType.TIMESTAMP:
                    return typeof(DateTime);
                case DbricksType.FLOAT:
                    return typeof(float);
                case DbricksType.DECIMAL:
                    return typeof(decimal);
                case DbricksType.DOUBLE:
                    return typeof(double);
                case DbricksType.INTEGER:
                    return typeof(int);
                case DbricksType.BINARY:
                    return typeof(byte[]);
                case DbricksType.SMALLINT:
                    return typeof(Int16);
                case DbricksType.TINYINT:
                    return typeof(byte);
                case DbricksType.BOOLEAN:
                    return typeof(bool);
                default:
                    return typeof(string);
            }
        }


        private async Task<DbricksResponse> SendAsync(string requestUri, int? limit, CancellationToken cancellationToken)
        {
            var contentType = "application/json";

            // Create the request message
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, requestUri);

            // Adding others headers
            if (this.Connection.CustomHeaders != null && this.Connection.CustomHeaders.Count > 0)
                foreach (var kvp in this.Connection.CustomHeaders)
                    if (!requestMessage.Headers.Contains(kvp.Key))
                        requestMessage.Headers.Add(kvp.Key, kvp.Value);

            // Check if data is null
            var binaryData = BuildContent(limit);

            // get byte array content
            requestMessage.Content = new ByteArrayContent(binaryData);

            // If Json, specify header
            if (!string.IsNullOrEmpty(contentType) && !requestMessage.Content.Headers.Contains("content-type"))
                requestMessage.Content.Headers.Add("content-type", contentType);

            DbricksResponse dbricksResponse = default;

            // Eventually, send the request
            var response = await this.Connection.HttpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            // throw exception if response is not successfull
            // get response from server
            if (!response.IsSuccessStatusCode && response.Content != null)
                await HandleSyncError(response);

            using (var streamResponse = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false))
            {
                if (streamResponse.CanRead)
                    dbricksResponse = await JsonSerializer.DeserializeAsync<DbricksResponse>(streamResponse, cancellationToken: cancellationToken);
            }

            if (dbricksResponse.Status.State == "FAILED")
                throw new WebException(dbricksResponse.Status.Error.Message, WebExceptionStatus.UnknownError);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            return dbricksResponse;
        }

        private async Task HandleSyncError(HttpResponseMessage response)
        {
            try
            {
                Exception exception = null;
                using var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                if (streamResponse.CanRead)
                {
                    // Error are always json formatted

                    var streamReader = new StreamReader(streamResponse);
                    var errorString = await streamReader.ReadToEndAsync().ConfigureAwait(false);

                    if (errorString != null)
                        exception = new Exception(errorString);
                    else
                        exception = new Exception(response.ReasonPhrase);

                }
                else
                {
                    exception = new Exception(response.ReasonPhrase);

                }

                //exception.ReasonPhrase = response.ReasonPhrase;
                //exception.StatusCode = response.StatusCode;

                throw exception;


            }
            catch (Exception)
            {
                throw;
            }


        }

        private string BuildUri(string baseUri, string path)
        {
            var requestUri = new StringBuilder();
            requestUri.Append(baseUri);
            if (!string.IsNullOrEmpty(path))
            {
                if (!baseUri.EndsWith("/"))
                    requestUri.Append('/');

                if (path.StartsWith("/", StringComparison.CurrentCultureIgnoreCase))
                    requestUri.Append(path.AsSpan(1));
                else
                    requestUri.Append(path);
            }

            // Remove trailing slash at end of requestUri
            if (requestUri[^1] == '/')
                requestUri.Remove(requestUri.Length - 1, 1);



            // Add params if any
            if (this.Connection.QueryParameters != null && this.Connection.QueryParameters.Count > 0)
            {
                string prefix = "?";
                foreach (var kvp in this.Connection.QueryParameters)
                {
                    requestUri.AppendFormat("{0}{1}={2}", prefix, Uri.EscapeDataString(kvp.Key),
                                            Uri.EscapeDataString(kvp.Value));
                    if (prefix.Equals("?"))
                        prefix = "&";
                }
            }

            return requestUri.ToString();
        }

    }
}
