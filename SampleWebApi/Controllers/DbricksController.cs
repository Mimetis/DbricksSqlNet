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
        [Route("Customers")]
        public async Task<JsonResult> GetCustomersAsync()
        {
            var command = new SqlWarehouseCommand(connection, "select * from samples.tpch.customer");
            var json = await command.LoadJsonAsync(10);

            return new JsonResult(json);
        }
    }
}
