using Databricks.Sql.Net.Command;
using Databricks.Sql.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Data;
using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text.Json.Nodes;

namespace SampleWebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DbricksController : ControllerBase
    {
        private readonly DbricksConnection connection;

        public DbricksController(DbricksConnection connection)
        {
            this.connection = connection;
        }

        [HttpGet(Name = "GetCustomers")]
        public async Task<JsonResult> GetCustomersAsync()
        {
            var command = new DbricksCommand(connection, "select * from samples.tpch.customer");
            var json = await command.LoadJsonAsync();

            return new JsonResult(json);
        }
    }
}
