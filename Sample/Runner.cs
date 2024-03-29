using Databricks.Sql.Net;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sample
{
    public class Runner : BackgroundService
    {
        public DbricksOptions Options { get; }
        public Runner(IOptions<DbricksOptions> options)
        {
            this.Options = options.Value;
            // do something with connection
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {

            var t = Type.GetType("System.Int64");

            Console.WriteLine("Options Host: " + Options.Host);
            Console.WriteLine("Options ApiKey: " + Options.ApiKey);
            Console.WriteLine("Options WarehouseId: " + Options.WarehouseId);
            Console.WriteLine("Options Catalog: " + Options.Catalog);
            Console.WriteLine("Options Schema: " + Options.Schema);
            Console.WriteLine();

            return Task.CompletedTask;
        }
    }
}
