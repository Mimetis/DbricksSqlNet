using Azure.Identity;
using Castle.Core.Configuration;
using Databricks.Sql.Net.Client;
using Databricks.Sql.Net.Options;
using Microsoft.Extensions.Options;
using Moq;
using System.Net.Http;

namespace Databricks.Sql.Net.Tests
{
    public class SqlWarehouseConnectionTests
    {
        private readonly SqlWarehouseOptions options = new()
        {
            ApiKey = "test",
            Catalog = "test",
            Host = "http://www.test.com",
            Schema = "test",
            WarehouseId = "test",
            WaitTimeout = 30,
            TenantId = "test",
            ManagedIdentityClientId = "test"
        };


        [Fact]
        public void Constructor_Validate_AsExpected()
        {
            var connection = new SqlWarehouseConnection(options);

            // Assert
            Assert.NotNull(connection);
            Assert.NotNull(connection.Options);
            Assert.NotNull(connection.HttpClient);
            Assert.NotNull(connection.Policy);
            Assert.NotNull(connection.RootUri);
            Assert.NotNull(connection.Authentication);
        }

        [Fact]
        public void Constructor_Validate_WithCustom_HttpClient_AsExpected()
        {
            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri("http://www.test.com");

            var connection = new SqlWarehouseConnection(options, httpClient);

            // Assert
            Assert.NotNull(connection.HttpClient);
            Assert.Equal(httpClient, connection.HttpClient);
            Assert.Equal(httpClient.BaseAddress, connection.HttpClient.BaseAddress);
        }

        [Fact]
        public void Constructor_Validate_GetPath()
        {
            var connection = new SqlWarehouseConnection(options);

            var expectedPath = new Uri(new Uri(options.Host), "/api/test");
            var path = connection.GetPath("/api/test");

            Assert.Equal(expectedPath, path);
        }

        [Fact]
        public void Constructor_Validate_GetPathWithQueryParmaters()
        {
            var connection = new SqlWarehouseConnection(options);

            var expectedPath = new Uri(new Uri(options.Host), "/api/2.0/sql/statements/xxxx-aaa-xxxx");
            var path = connection.GetSqlStatementsResultPath("xxxx-aaa-xxxx");

            Assert.Equal(expectedPath, path);
        }



    }
}