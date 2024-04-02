using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net
{
    public class DbricksConnection
    {
        internal const string JobsRunNowPath = "/api/2.1/jobs/run-now";
        internal const string JobsRunsGetPath = "/api/2.1/jobs/runs/get?run_id={0}";
        internal const string JobsRunsGetOutputPath = "/api/2.1/jobs/runs/get-output?run_id={0}";
        internal const string SqlStatementsPath = "/api/2.0/sql/statements";
        internal const string SqlStatementsResultPath = "/api/2.0/sql/statements/{0}";

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


        public DbricksOptions Options { get; }

        /// <summary>
        /// Gets the root uri of the databricks instance, based on option name Host
        /// </summary>
        public Uri RootUri { get; }


        private Uri GetPath(string relativePath) => new(RootUri, relativePath);

        //private Uri GetPathWithQueryParmaters(string relativePath, params object[] queryParameters) => GetPath(string.Format(relativePath, queryParameters));

        public Uri GetJobsRunNowPath() => GetPath(JobsRunNowPath);

        //public Uri GetJobsRunsGetPath(long runId) => GetPathWithQueryParmaters(JobsRunsGetPath, runId);

        //public Uri GetJobsRunsGetOutputPath(long runId) => GetPathWithQueryParmaters(JobsRunsGetOutputPath, runId);

        public Uri GetSqlStatementsPath() => GetPath(SqlStatementsPath);

        //public Uri GetSqlStatementsResultPath(string statementId) => GetPathWithQueryParmaters(SqlStatementsResultPath, statementId);

        public DbricksConnection(IOptions<DbricksOptions> dbriksOptions, HttpClient client = null, Policy policy = null)
            : this(dbriksOptions.Value, client, policy)
        {
        }

        public DbricksConnection(DbricksOptions dbriksOptions, HttpClient client = null, Policy policy = null)
        {
            this.Policy = this.EnsurePolicy(policy);
            this.Options = dbriksOptions;
            this.RootUri = new Uri(this.Options.Host);

            // if no HttpClient provisionned, create a new one
            if (client == null)
            {
                var handler = new HttpClientHandler();

                // Activated by default
                if (handler.SupportsAutomaticDecompression)
                    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

                this.HttpClient = new HttpClient(handler);

                this.HttpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {this.Options.ApiKey}");
            }
            else
            {
                this.HttpClient = client;
            }
        }

        private Policy EnsurePolicy(Policy policy)
        {
            if (policy != default)
                return policy;

            // Defining my retry policy
            policy = Policy.WaitAndRetry(1, TimeSpan.FromMilliseconds(500));

            return policy;
        }
    }
}
