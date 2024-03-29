using DataBrickConnector.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
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

        private byte[] BuildContent()
        {

            var obj = new
            {
                warehouse_id = this.Connection.Options.WarehouseId,
                catalog = this.Connection.Options.Catalog,
                schema = this.Connection.Options.Schema,
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

        public async Task<T> ExecuteAsync<T>(IProgress<T> progress = default, CancellationToken cancellationToken = default)
        {
            if (this.Connection.HttpClient is null)
                throw new ArgumentNullException(nameof(this.Connection.HttpClient));

            if (this.Connection.Options.Host == null)
                throw new ArgumentException("Host is not defined");

            // Build the request uri to Sql Statements
            var requestUri = BuildUri(this.Connection.Options.Host, DbricksConnection.SqlStatementsPath);

            HttpResponseMessage response = null;
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Execute my OpenAsync in my policy context
                response = await this.Connection.Policy.ExecuteAsync(ct => this.SendAsync(requestUri, ct), progress, cancellationToken);

                if (response.Content == null)
                    throw new Exception("response content is null");

                DbricksResponse dbricksResponse = default;

                using (var streamResponse = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (streamResponse.CanRead)
                        dbricksResponse = await JsonSerializer.DeserializeAsync<DbricksResponse>(streamResponse, cancellationToken: cancellationToken);
                }

                var jsonResult = dbricksResponse.ToJsonArray();
                var result = JsonSerializer.Deserialize<T>(jsonResult);

                return result;
            }
            catch (Exception e)
            {
                if (response == null || response.Content == null)
                    throw new Exception(e.Message);

                var exrror = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

                throw new Exception(exrror);
            }

        }


        private async Task<HttpResponseMessage> SendAsync(string requestUri, CancellationToken cancellationToken)
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
            var binaryData = BuildContent();

            // get byte array content
            requestMessage.Content = new ByteArrayContent(binaryData);

            // If Json, specify header
            if (!string.IsNullOrEmpty(contentType) && !requestMessage.Content.Headers.Contains("content-type"))
                requestMessage.Content.Headers.Add("content-type", contentType);

            HttpResponseMessage response;
            try
            {
                // Eventually, send the request
                response = await this.Connection.HttpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();

            // throw exception if response is not successfull
            // get response from server
            if (!response.IsSuccessStatusCode && response.Content != null)
                await HandleSyncError(response);

            return response;

        }

        /// <summary>
        /// Handle a request error
        /// </summary>
        /// <returns></returns>
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
