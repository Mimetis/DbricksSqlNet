using Databricks.Sql.Net.Models.Sql;
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
        public async Task<List<T>> LoadAsync<T>(int? limit = default, IProgress<StatementProgress> progress = default, CancellationToken cancellationToken = default)
        {

            var list = new List<T>();

            await foreach (var item in GetJsonObjectsAsync(limit, progress, cancellationToken))
                list.Add(JsonSerializer.Deserialize<T>(item));

            return list;
        }

        /// <summary>
        /// Load the result of the command into a JsonArray
        /// </summary>
        public async Task<JsonArray> LoadJsonAsync(int? limit = default, IProgress<StatementProgress> progress = default, CancellationToken cancellationToken = default)
        {

            var list = new JsonArray();

            await foreach (var item in GetJsonObjectsAsync(limit, progress, cancellationToken))
                list.Add(item);

            return list;
        }

        /// <summary>
        /// Return an enumerable of JsonObject from the command
        /// </summary>
        public async IAsyncEnumerable<JsonObject> GetJsonObjectsAsync(int? limit = default, IProgress<StatementProgress> progress = default, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {

            var token = await this.Connection.Authentication.GetTokenAsync(cancellationToken).ConfigureAwait(false);

            if (string.IsNullOrEmpty(token))
                throw new WebException("Token is null or empty", WebExceptionStatus.UnknownError);

            // Get the response from the server
            var dbricksResponse = await ExecuteAsync(limit, token, progress, cancellationToken);

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
                    ct => this.Connection.SendAsync<StatementResult>(nextChunkUri, HttpMethod.Get, token, default, ct), default, cancellationToken).ConfigureAwait(false);

                SendProgress(progress, dbricksResponse.Status?.State, dbricksResponse.StatementId, dbricksResponse.Manifest?.TotalRowCount, dbricksResponse.Manifest?.TotalChunkCount, chunk);

                foreach (var row in GetJsonValues(chunk.DataArray, columnsType))
                    yield return row;

                nextChunkInternalLink = chunk.NextChunkInternalLink;
            }

            yield break;
        }

        private static Dictionary<string, (Type Type, int Position)> GetColumns(Statement dbricksResponse)
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
                SqlWarehouseType.INT => typeof(int),
                //SqlWarehouseType.BINARY => typeof(byte[]),
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
        public async Task<Statement> ExecuteAsync(int? limit = 1000, string token = default, IProgress<StatementProgress> progress = default, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(token))
                token = await this.Connection.Authentication.GetTokenAsync(cancellationToken).ConfigureAwait(false);

            if (string.IsNullOrEmpty(token))
                throw new WebException("Token is null or empty", WebExceptionStatus.UnknownError);


            var dbricksResponse = await this.Connection.Policy.ExecuteAsync(
                ct => this.Connection.SendAsync<Statement>(this.Connection.GetSqlStatementsPath(), HttpMethod.Post, token, this.BuildRequestContent(limit), ct), default, cancellationToken).ConfigureAwait(false);

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
                    ct => this.Connection.SendAsync<Statement>(requestStatementUri, HttpMethod.Get, token, cancellationToken:ct), default, cancellationToken).ConfigureAwait(false);

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

        private static void SendProgress(IProgress<StatementProgress> progress, string status, string statementId, int? totalRowCount, int? totalChunkCount, StatementResult result)
        {
            progress?.Report(new StatementProgress
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

        public byte[] BuildRequestContent(int? limit = 1000)
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

    }
}
