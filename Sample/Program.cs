using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.IO;
using Databricks.Sql.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Databricks.Sql.Net.Command;
using System.Collections.Generic;
using Databricks.Sql.Net.Enumeration;
using System;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using System.Text;
using DataBrickConnector.Models;
using System.Text.Json;

namespace Sample
{
    internal class Program
    {

        private static IConfiguration configuration;

        static async Task Main(string[] args)
        {
            //var builder = Host.CreateApplicationBuilder(args);
            //builder.Services.AddHttpClient();
            //builder.Services.AddOptions();
            //builder.Services.AddDataBricksSqlNet(builder.Configuration);
            //builder.Services.AddHostedService<Runner>();

            //var host = builder.Build();
            //host.Run();

            // read configuration from appsettings.json and load into IConfiguration
            configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appSettings.json", false, true)
                .AddJsonFile("appSettings.local.json", true, true)
                .Build();

            // using configuration get the Databricks options
            var options = new DbricksOptions();
            configuration.GetSection("Databricks").Bind(options);

            // print the options
            Console.WriteLine("Options Host: " + options.Host);

            //await TestDatabricksSqlNetAffectedOrdersAsync(options);

            await TestDatabricksSqlNetAlertGroupsAsync(options);
        }

        public static async Task TestDatabricksSqlNetAffectedOrdersAsync(DbricksOptions options)
        {
            // create a DbricksConnection object with the IOPtions parameter
            var connection = new DbricksConnection(options);

            // create a DbricksCommand object with the connection parameter

            var command = new DbricksCommand(connection, AffectedOrders.GetAffectedOrdersStatement());
            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "exp",
                Value = "2024-03-29",
                Type = DbricksType.DATE,
            });

            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "loc",
                Value = "6106",
                Type = DbricksType.STRING,
            });

            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "sku",
                Value = "000000000002289201",
                Type = DbricksType.STRING,
            });

            // first to start server
            var result = await command.ExecuteAsync<List<AffectedOrders>>();


            var stopWatch = Stopwatch.StartNew();
            result = await command.ExecuteAsync<List<AffectedOrders>>();
            stopWatch.Stop();
            Console.WriteLine($"[Ellapsed Time :{stopWatch.Elapsed.Minutes}:{stopWatch.Elapsed.Seconds}.{stopWatch.Elapsed.Milliseconds}]");
            Console.WriteLine($"Affected orders count : {result.Count}");

            command = new DbricksCommand(connection, AffectedOrders.GetAffectedOrdersStatement());
            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "exp",
                Value = "2024-03-30",
                Type = DbricksType.DATE,
            });

            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "loc",
                Value = "6106",
                Type = DbricksType.STRING,
            });

            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "sku",
                Value = "000000000002289201",
                Type = DbricksType.STRING,
            });


            stopWatch = Stopwatch.StartNew();
            result = await command.ExecuteAsync<List<AffectedOrders>>();
            stopWatch.Stop();
            Console.WriteLine($"[Ellapsed Time :{stopWatch.Elapsed.Minutes}:{stopWatch.Elapsed.Seconds}.{stopWatch.Elapsed.Milliseconds}]");
            Console.WriteLine($"Affected orders count : {result.Count}");

            command = new DbricksCommand(connection, AffectedOrders.GetAffectedOrdersStatement());
            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "exp",
                Value = "2024-03-31",
                Type = DbricksType.DATE,
            });

            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "loc",
                Value = "6106",
                Type = DbricksType.STRING,
            });

            command.Parameters.Add(new DbricksCommandParameter
            {
                ParameterName = "sku",
                Value = "000000000002289201",
                Type = DbricksType.STRING,
            });


            stopWatch = Stopwatch.StartNew();
            result = await command.ExecuteAsync<List<AffectedOrders>>();
            stopWatch.Stop();
            Console.WriteLine($"[Ellapsed Time :{stopWatch.Elapsed.Minutes}:{stopWatch.Elapsed.Seconds}.{stopWatch.Elapsed.Milliseconds}]");
            Console.WriteLine($"Affected orders count : {result.Count}");
        }

        public static async Task TestDatabricksSqlNetAlertGroupsAsync(DbricksOptions options)
        {
            // create a DbricksConnection object with the IOPtions parameter
            var connection = new DbricksConnection(options);

            // create a DbricksCommand object with the connection parameter

            var command = new DbricksCommand(connection, AlertGroupRepository.GetAlertGroupsStringWithParameters());

            command.Parameters.Add(new DbricksCommandParameter { ParameterName = "from", Type = DbricksType.DATE });
            command.Parameters.Add(new DbricksCommandParameter { ParameterName = "to", Type = DbricksType.DATE });

            var globalStopWatch = Stopwatch.StartNew();

            for (int i = 1; i < 20; i++)
            {
                // format a date with i as day. if i < 10, should add a 0 before i
                var start = new DateOnly(2024, 4, i);
                var end = new DateOnly(2024, 4, i);

                command.Parameters[0].Value = start;
                command.Parameters[1].Value = end;
                var stopWatch = Stopwatch.StartNew();
                var result = await command.ExecuteAsync<List<AlertGroup>>();
                stopWatch.Stop();
                Console.WriteLine($"[Ellapsed Time :{stopWatch.Elapsed.Minutes}:{stopWatch.Elapsed.Seconds}.{stopWatch.Elapsed.Milliseconds}]");
                Console.WriteLine($"Alert groups count : {result.Count}");
            }
            globalStopWatch.Stop();
            Console.WriteLine($"[Overall elapsed Time :{globalStopWatch.Elapsed.Minutes}:{globalStopWatch.Elapsed.Seconds}.{globalStopWatch.Elapsed.Milliseconds}]");

        }
    }
}
