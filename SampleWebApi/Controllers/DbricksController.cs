using Databricks.Sql.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Data;
using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text.Json.Nodes;
using Azure.Identity;
using Azure.Core;
using Databricks.Sql.Net.Client;
using Databricks.Sql.Net.Options;
using System.Text.Json;
using System.Net.Http;
using System.Collections.Generic;
using Databricks.Sql.Net.Models.Sql;

namespace SampleWebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DbricksController : ControllerBase
    {
        private readonly SqlWarehouseConnection connection;
        private readonly SqlWarehouseOptions options;
        private readonly string dbricksResources;

        public DbricksController(SqlWarehouseConnection connection, IOptions<SqlWarehouseOptions> options)
        {
            this.connection = connection;
            this.options = options.Value;
        }


        [HttpGet()]
        [Route("Environment")]
        public Task<JsonResult> GetEnvironmentAsync()
        {

            // get if we are in dev mode or prod mode
            var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

            return Task.FromResult(new JsonResult(env));
        }


        [HttpGet()]
        [Route("Options")]
        public Task<JsonResult> GetOptionsAsync()
        {

            // get if we are in dev mode or prod mode
            return Task.FromResult(new JsonResult(options));
        }



        [HttpGet()]
        [Route("UserAssignedIdentity")]
        public async Task<JsonResult> GetUserAssignedIdentityAsync()
        {
            var azureAuthOptions = new DefaultAzureCredentialOptions
            {
                ExcludeInteractiveBrowserCredential = false,
                ManagedIdentityClientId = options.ManagedIdentityClientId,
                TenantId = options.TenantId,
            };

            var credential = new DefaultAzureCredential(azureAuthOptions);

            var token = await credential.GetTokenAsync(new TokenRequestContext([$"{Constants.DatabricksResourceId}/.default"]));

            return new JsonResult(token.Token);
        }



        [HttpGet()]
        [Route("SystemAssignedIdentity")]
        public async Task<JsonResult> GetSystemAssignedIdentityAsync()
        {
            var azureAuthOptions = new DefaultAzureCredentialOptions { TenantId = options.TenantId, };
            var credential = new DefaultAzureCredential(azureAuthOptions);

            var token = await credential.GetTokenAsync(new TokenRequestContext([$"{Constants.DatabricksResourceId}/.default"]));

            return new JsonResult(token.Token);
        }



        [HttpGet()]
        [Route("DbricksAuth")]
        public async Task<JsonResult> GetDbricksAuthAsync()
        {
            // get the available token
            var token = await connection.Authentication.GetTokenAsync();
            return new JsonResult(token);
        }

        [HttpGet()]
        [Route("LineItems")]
        public async Task<JsonResult> GetLineItemsAsync(int count = 100000)
        {
            var progress = new Progress<StatementProgress>();
            progress.ProgressChanged += (sender, e) => Debug.WriteLine(e);

            var command = new SqlWarehouseCommand(connection, "SELECT l_orderkey, l_extendedprice, l_shipdate FROM lineitem");
            var json = await command.LoadJsonAsync(count, progress);

            return new JsonResult(json);
        }


        [HttpGet()]
        [Route("DbricksResponse")]
        public async Task<JsonResult> GetDbricksResponseAsync(int count = 3)
        {
            var command = new SqlWarehouseCommand(connection, "SELECT l_orderkey, l_extendedprice, l_shipdate FROM lineitem");
            var json = await command.ExecuteAsync(3);
            return new JsonResult(json);
        }


        [HttpGet()]
        [Route("Warehouses")]
        public Task<List<Warehouse>> GetWarehousesAsync() => connection.GetSqlWarehousesAsync();

        [HttpGet()]
        [Route("Queries")]
        public Task<Query> GetQueriesAsync() => connection.GetSqlWarehousesQueriesAsync();


        [HttpGet()]
        [Route("QueriesHistory")]
        public Task<QueryHistory> GetQueriesHistoryAsync() => connection.GetSqlWarehousesQueriesHystoryAsync(includeMetrics:true);

        [HttpGet()]
        [Route("WarehousePermissions")]
        public Task<WarehousePermissions> GetWarehousePermissionsAsync(string warehouseId) => connection.GetSqlWarehousePermissionsAsync(warehouseId);

        [HttpGet()]
        [Route("Schema")]
        public async Task<Schema> GetSchemaAsync() => await connection.GetSchemaAsync();

        [HttpGet()]
        [Route("Table")]
        public async Task<Table> GetTableAsync(string tableName) => await connection.GetTableAsync(tableName);

    }
}
