using Databricks.Sql.Net.Models;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;


namespace Databricks.Sql.Net.Client
{
    public class SqlWarehouseCommand
    {

        /// <summary>
        /// Gets or Sets the command parameters.
        /// </summary>
        public List<SqlWarehouseParameter> Parameters { get; set; } = [];

        /// <summary>
        /// Gets or sets the databricks connection.
        /// </summary>
        public SqlWarehouseConnection Connection { get; set; }

        /// <summary>
        /// Gets or Sets the command text to be executed.
        /// </summary>
        public string CommandText { get; set; }

        /// <summary>
        /// Gets or Sets the wait timeout in seconds.
        /// If null, gets the default value from the options parameters.
        /// </summary>
        public int? WaitTimeout { get; set; }

        public SqlWarehouseCommand()
        {

        }

        public SqlWarehouseCommand(SqlWarehouseConnection connetion, string commandText, int? waitTimeout = null)
        {
            this.CommandText = commandText;
            this.Connection = connetion;
            this.WaitTimeout = waitTimeout;
        }

        /// <summary>
        /// Load the result of the command into a generic type T
        /// </summary>
        public async Task<List<T>> LoadAsync<T>(int? limit = default, IProgress<SqlWarehouseProgress> progress = default, CancellationToken cancellationToken = default)
        {

            var list = new List<T>();

            await foreach (var item in GetJsonObjectsAsync(limit, progress, cancellationToken))
                list.Add(JsonSerializer.Deserialize<T>(item));

            return list;
        }

        /// <summary>
        /// Load the result of the command into a JsonArray
        /// </summary>
        public async Task<JsonArray> LoadJsonAsync(int? limit = default, IProgress<SqlWarehouseProgress> progress = default, CancellationToken cancellationToken = default)
        {

            var list = new JsonArray();

            await foreach (var item in GetJsonObjectsAsync(limit, progress, cancellationToken))
                list.Add(item);

            return list;
        }

        /// <summary>
        /// Return an enumerable of JsonObject from the command
        /// </summary>
        public async IAsyncEnumerable<JsonObject> GetJsonObjectsAsync(int? limit = default, IProgress<SqlWarehouseProgress> progress = default, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Build the request uri to Sql Statements
            var requestUri = this.Connection.GetSqlStatementsPath();

            // get token
            var token = await this.Connection.Authentication.GetTokenAsync(cancellationToken).ConfigureAwait(false);

            if (string.IsNullOrEmpty(token))
                throw new InvalidOperationException("Token is null or empty");

            // Build data to send
            var binaryData = BuildRequestContent(limit);

            // Get the response from the server
            var dbricksResponse = await ExecuteAsync(requestUri, HttpMethod.Post, token, binaryData, progress, cancellationToken);

            // get columns with type
            var columnsType = GetColumns(dbricksResponse);

            foreach (var row in GetJsonValues(dbricksResponse.Result.DataArray, columnsType))
                yield return row;

            // read next chunks if any
            var nextChunkInternalLink = dbricksResponse.Result.NextChunkInternalLink;

            while (!string.IsNullOrEmpty(nextChunkInternalLink))
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                var nextChunkUri = this.Connection.GetPath(nextChunkInternalLink);

                var chunk = await this.Connection.Policy.ExecuteAsync(
                    ct => this.SendAsync<SqlWarehouseResult>(nextChunkUri, HttpMethod.Get, token, default, ct), default, cancellationToken).ConfigureAwait(false);

                SendProgress(progress, dbricksResponse.Status?.State, dbricksResponse.StatementId, dbricksResponse.Manifest?.TotalRowCount, dbricksResponse.Manifest?.TotalChunkCount, chunk);

                foreach (var row in GetJsonValues(chunk.DataArray, columnsType))
                    yield return row;

                nextChunkInternalLink = chunk.NextChunkInternalLink;
            }

            yield break;
        }

        private static Dictionary<string, (Type Type, int Position)> GetColumns(SqlWarehouseResponse dbricksResponse)
        {
            // gets columns type based on dbricksResponse.Manifest.Schema.Columns columns
            var columnsType = new Dictionary<string, (Type Type, int Position)>();

            // Get types from dbricksResponse.Manifest.Schema.Columns
            foreach (var column in dbricksResponse.Manifest.Schema.Columns)
            {
                var type = typeof(string);
                var columTypeText = column.TypeText.IndexOf('(') > 0 ? column.TypeText[..column.TypeText.IndexOf('(')] : column.TypeText;

                if (Enum.TryParse(columTypeText, out SqlWarehouseType dbricksType))
                    type = GetTypeFromDbricksType(dbricksType);

                columnsType.Add(column.Name, (Type: type, column.Position));
            }

            return columnsType;
        }

        /// <summary>
        /// Returns a list of JsonObjects from a data array
        /// </summary>
        internal static IEnumerable<JsonObject> GetJsonValues(string[][] dataArray, Dictionary<string, (Type Type, int Position)> columnsType)
        {
            if (dataArray == null || dataArray.Length <= 0)
                yield break;

            foreach (var row in dataArray)
            {
                var jsonRow = new JsonObject();
                foreach (var column in columnsType)
                {
                    var value = TypeConverter.TryConvertTo(row[column.Value.Position], column.Value.Type);
                    jsonRow[column.Key] = JsonValue.Create(value);
                }
                yield return jsonRow;
            }
        }

        /// <summary>
        /// Get the type from the DbricksType enumeration
        /// </summary>
        private static Type GetTypeFromDbricksType(SqlWarehouseType dbricksType)
        {
            // return dotnet type from enumeration DbricksType
            return dbricksType switch
            {
                SqlWarehouseType.BIGINT => typeof(long),
                SqlWarehouseType.VOID => typeof(DBNull),
                SqlWarehouseType.STRING => typeof(string),
                SqlWarehouseType.DATE => typeof(DateOnly),
                SqlWarehouseType.TIMESTAMP or SqlWarehouseType.TIMESTAMP_NTZ => typeof(DateTime),
                SqlWarehouseType.FLOAT => typeof(float),
                SqlWarehouseType.DECIMAL => typeof(decimal),
                SqlWarehouseType.DOUBLE => typeof(double),
                SqlWarehouseType.INTEGER or SqlWarehouseType.INT => typeof(int),
                SqlWarehouseType.BINARY => typeof(byte[]),
                SqlWarehouseType.SMALLINT => typeof(short),
                SqlWarehouseType.TINYINT => typeof(byte),
                SqlWarehouseType.BOOLEAN => typeof(bool),
                _ => typeof(string),
            };
        }


        /// <summary>
        /// Execute the command and return the result as a SqlWarehouseResponse instance
        /// IF needed (status == PENDING), execute the command multiple times to get the data
        /// </summary>
        public async Task<SqlWarehouseResponse> ExecuteAsync(Uri requestUri, HttpMethod method, string token, byte[] binaryData = default, IProgress<SqlWarehouseProgress> progress = default, CancellationToken cancellationToken = default)
        {

            var dbricksResponse = await this.Connection.Policy.ExecuteAsync(
                ct => this.SendAsync<SqlWarehouseResponse>(requestUri, method, token, binaryData, ct), default, cancellationToken).ConfigureAwait(false);

            if (dbricksResponse == null)
                return default;

            var status = dbricksResponse.Status.State;

            if (status == "FAILED")
                throw new WebException(dbricksResponse.Status.Error.Message, WebExceptionStatus.UnknownError);

            SendProgress(progress, status, dbricksResponse.StatementId, dbricksResponse.Manifest?.TotalRowCount, dbricksResponse.Manifest?.TotalChunkCount, dbricksResponse.Result);

            int cpt = 0;
            while (status == "PENDING" || status == "RUNNING" && cpt < 10)
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // wait 1s + 1s * cpt
                await Task.Delay(1000 + (cpt * 1000), cancellationToken);

                // get the uri to the statement result
                var requestStatementUri = this.Connection.GetSqlStatementsResultPath(dbricksResponse.StatementId);

                // Execute my OpenAsync in my policy context
                dbricksResponse = await this.Connection.Policy.ExecuteAsync(
                    ct => this.SendAsync<SqlWarehouseResponse>(requestStatementUri, HttpMethod.Get, token, binaryData, ct), default, cancellationToken).ConfigureAwait(false);

                // get status and increment counter to break the loop if needed
                status = dbricksResponse.Status.State;
                cpt++;

                // send progress
                SendProgress(progress, status, dbricksResponse.StatementId, dbricksResponse.Manifest?.TotalRowCount, dbricksResponse.Manifest?.TotalChunkCount, dbricksResponse.Result);
            }

            if (cpt >= 10 || status == "PENDING" || status == "RUNNING")
                throw new WebException("Timeout", WebExceptionStatus.Timeout);

            if (status == "FAILED")
                throw new WebException(dbricksResponse.Status.Error.Message, WebExceptionStatus.UnknownError);

            return dbricksResponse;
        }

        internal async Task<T> SendAsync<T>(Uri requestUri, HttpMethod method, string token, byte[] binaryData = default, CancellationToken cancellationToken = default)
        {
            // Create the request message
            var requestMessage = new HttpRequestMessage(method, requestUri);

            // Add the Authorization header
            requestMessage.Headers.Add("Authorization", $"Bearer {token}");

            // Adding others headers
            if (this.Connection.CustomHeaders != null && this.Connection.CustomHeaders.Count > 0)
                foreach (var kvp in this.Connection.CustomHeaders)
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
            var response = await this.Connection.HttpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            // throw exception if response is not successfull
            // get response from server
            if (!response.IsSuccessStatusCode)
                await HandleSyncErrorAsync(response);

            using (var streamResponse = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false))
            {
                if (streamResponse.CanRead)
                    res = await JsonSerializer.DeserializeAsync<T>(streamResponse, cancellationToken: cancellationToken);
            }

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            return res;
        }

        private static void SendProgress(IProgress<SqlWarehouseProgress> progress, string status, string statementId, int? totalRowCount, int? totalChunkCount, SqlWarehouseResult result)
        {
            progress?.Report(new SqlWarehouseProgress
            {
                ChunkRowCount = result?.RowCount,
                ChunkRowOffset = result?.RowOffset,
                ChunkIndex = result?.ChunkIndex,
                NextChunkIndex = result?.NextChunkIndex,
                NextChunkInternalLink = result?.NextChunkInternalLink,
                ExternalLinks = result?.ExternalLinks,
                TotalRowCount = totalRowCount,
                TotalChunkCount = totalChunkCount,
                StatementId = statementId,
                State = status
            });
        }

        public byte[] BuildRequestContent(int? limit)
        {

            var databricksQueryContent = new
            {
                warehouse_id = this.Connection.Options.WarehouseId,
                catalog = this.Connection.Options.Catalog,
                schema = this.Connection.Options.Schema,
                row_limit = limit ?? (int?)null,
                format = "JSON_ARRAY",
                disposition = "INLINE",
                wait_timeout = this.WaitTimeout.HasValue ? $"{this.WaitTimeout.Value}s" : $"{this.Connection.Options.WaitTimeout}s",
                statement = this.CommandText,
                parameters = this.Parameters == null || this.Parameters.Count <= 0 ? [] : this.Parameters.Select(p => new
                {
                    name = p.ParameterName,
                    type = p.TypeName,
                    value = p.Value

                }).ToArray(),
            };

            return JsonSerializer.SerializeToUtf8Bytes(databricksQueryContent);
        }

        private static async Task HandleSyncErrorAsync(HttpResponseMessage response)
        {
            try
            {
                Exception exception = null;

                if (response.Content == null)
                    throw new Exception(response.ReasonPhrase);

                using var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                if (!streamResponse.CanRead)
                    throw new Exception(response.ReasonPhrase);

                var streamReader = new StreamReader(streamResponse);
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
