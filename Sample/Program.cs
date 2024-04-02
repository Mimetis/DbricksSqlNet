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
using System.Data;

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

            await GetCustomersAsync(options);

        }

        private static async Task GetCustomersAsync(DbricksOptions options)
        {
            var connection = new DbricksConnection(options);
            var command = new DbricksCommand(connection, "select * from samples.tpch.lineitem");
            var datatable = await command.LoadDataTableAsync();

            foreach (DataRow row in datatable.Rows)
            {
                foreach (DataColumn column in datatable.Columns)
                    Console.Write($"{column.ColumnName}:{row[column.ColumnName]} - ");
                Console.WriteLine();
            }
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

            var stopWatch = Stopwatch.StartNew();
            var result = await command.LoadAsync<List<AffectedOrders>>();
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
            result = await command.LoadAsync<List<AffectedOrders>>();
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
            result = await command.LoadAsync<List<AffectedOrders>>();
            stopWatch.Stop();
            Console.WriteLine($"[Ellapsed Time :{stopWatch.Elapsed.Minutes}:{stopWatch.Elapsed.Seconds}.{stopWatch.Elapsed.Milliseconds}]");
            Console.WriteLine($"Affected orders count : {result.Count}");
        }
    }
}
