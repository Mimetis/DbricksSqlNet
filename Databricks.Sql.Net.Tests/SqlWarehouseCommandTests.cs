using Databricks.Sql.Net.Client;
using Moq.Protected;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using Azure;
using System.Net.Http;
using Databricks.Sql.Net.Options;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace Databricks.Sql.Net.Tests
{
    public class SqlWarehouseCommandTests
    {

        [Fact]
        public async Task Execute_ShouldWork()
        {
            var httpClient = Mockups.GetHttpClientMock(Mockups.DATABRICKS_JSON_ARRAY_RESPONSE);

            var options = new SqlWarehouseOptions { Host = "https://adb-xxxx-1.azuredatabricks.net", ApiKey = "eyj..." };
            var connection = new SqlWarehouseConnection(options, httpClient);

            var command = new SqlWarehouseCommand(connection, "SELECT * FROM lineitem");
            var content = command.BuildRequestContent(3);
            var requestUri = connection.GetSqlStatementsPath();

            // Act
            var dbricksResult = await command.ExecuteAsync(requestUri, HttpMethod.Post, "eyj...", content);

            // Assert
            Assert.NotNull(dbricksResult);
            Assert.Equal(3, dbricksResult.Result.RowCount);
            Assert.Equal("SUCCEEDED", dbricksResult.Status.State);
            Assert.Equal(8, dbricksResult.Manifest.Schema.ColumnCount);
            Assert.Equal(3, dbricksResult.Result.DataArray.Length);
        }


        [Fact]
        public async Task GetJsonsEnumerable_ShouldWork()
        {
            var httpClient = Mockups.GetHttpClientMock(Mockups.DATABRICKS_JSON_ARRAY_RESPONSE);

            var options = new SqlWarehouseOptions { Host = "https://adb-xxxx-1.azuredatabricks.net", ApiKey = "eyj..." };
            var connection = new SqlWarehouseConnection(options, httpClient);

            var command = new SqlWarehouseCommand(connection, "SELECT * FROM lineitem");
            await foreach (var jsonObject in command.GetJsonObjectsAsync(3))
            {
                var l_orderkey = jsonObject["l_orderkey"];
                Assert.NotNull(l_orderkey);
                Assert.Equal(JsonValueKind.Number, l_orderkey.GetValueKind());

                var l_linenumber = jsonObject["l_linenumber"];
                Assert.NotNull(l_linenumber);
                Assert.Equal(JsonValueKind.Number, l_linenumber.GetValueKind());

                var l_quantity = jsonObject["l_quantity"];
                Assert.NotNull(l_quantity);
                Assert.Equal(JsonValueKind.Number, l_quantity.GetValueKind());

                var l_returnflag = jsonObject["l_returnflag"];
                Assert.NotNull(l_returnflag);
                Assert.Equal(JsonValueKind.String, l_returnflag.GetValueKind());

                var l_shipdate = jsonObject["l_shipdate"];
                Assert.NotNull(l_shipdate);
                Assert.Equal(JsonValueKind.String, l_shipdate.GetValueKind());

                var l_distance = jsonObject["l_distance"];
                Assert.NotNull(l_distance);
                Assert.Equal(JsonValueKind.Number, l_distance.GetValueKind());

                var l_pickup_datetime = jsonObject["l_pickup_datetime"];
                Assert.NotNull(l_pickup_datetime);
                Assert.Equal(JsonValueKind.String, l_pickup_datetime.GetValueKind());

                var l_isvalid = jsonObject["l_isvalid"];
                Assert.NotNull(l_isvalid);
                Assert.Equal(JsonValueKind.True, l_isvalid.GetValueKind());
            }
        }

        [Fact]
        public async Task LoadJson_ShouldWork()
        {
            var httpClient = Mockups.GetHttpClientMock(Mockups.DATABRICKS_JSON_ARRAY_RESPONSE);

            var options = new SqlWarehouseOptions { Host = "https://adb-xxxx-1.azuredatabricks.net", ApiKey = "eyj..." };
            var connection = new SqlWarehouseConnection(options, httpClient);

            var command = new SqlWarehouseCommand(connection, "SELECT * FROM lineitem");
            var jsonArray = await command.LoadJsonAsync(3);

            Assert.NotNull(jsonArray);
            Assert.IsType<JsonArray>(jsonArray);
            Assert.Equal(3, jsonArray.Count);
            var jsonObject = jsonArray[0] as JsonObject;
            Assert.NotNull(jsonObject);

            var l_orderkey = jsonObject["l_orderkey"];
            Assert.NotNull(l_orderkey);
            Assert.Equal(JsonValueKind.Number, l_orderkey.GetValueKind());

            var l_linenumber = jsonObject["l_linenumber"];
            Assert.NotNull(l_linenumber);
            Assert.Equal(JsonValueKind.Number, l_linenumber.GetValueKind());

            var l_quantity = jsonObject["l_quantity"];
            Assert.NotNull(l_quantity);
            Assert.Equal(JsonValueKind.Number, l_quantity.GetValueKind());

            var l_returnflag = jsonObject["l_returnflag"];
            Assert.NotNull(l_returnflag);
            Assert.Equal(JsonValueKind.String, l_returnflag.GetValueKind());

            var l_shipdate = jsonObject["l_shipdate"];
            Assert.NotNull(l_shipdate);
            Assert.Equal(JsonValueKind.String, l_shipdate.GetValueKind());
        }

        [Fact]
        public async Task LoadOfT_ShouldWork()
        {
            var httpClient = Mockups.GetHttpClientMock(Mockups.DATABRICKS_JSON_ARRAY_RESPONSE);

            var options = new SqlWarehouseOptions { Host = "https://adb-xxxx-1.azuredatabricks.net", ApiKey = "eyj..." };
            var connection = new SqlWarehouseConnection(options, httpClient);

            var command = new SqlWarehouseCommand(connection, "SELECT * FROM lineitem");
            var lstItems = await command.LoadAsync<LineItem>(3);

            Assert.NotNull(lstItems);
            Assert.IsType<List<LineItem>>(lstItems);
            Assert.Equal(3, lstItems.Count);

            var lineItem = lstItems[0];
            Assert.NotNull(lineItem);

            Assert.IsType<long>(lineItem.OrderKey);
            Assert.IsType<int>(lineItem.LineNumber);
            Assert.IsType<decimal>(lineItem.Quantity);
            Assert.IsType<string>(lineItem.ReturnFlag);
            Assert.IsType<DateOnly>(lineItem.ShipDate);
            Assert.IsType<double>(lineItem.Distance);
            Assert.IsType<DateTime>(lineItem.PickupDatetime);
            Assert.IsType<bool>(lineItem.IsValid);

        }

        [Fact]
        public async Task LoadOfT_WithChunks_ShouldWork()
        {
            var httpClient = Mockups.GetHttpClientSequenceMock(Mockups.DATABRICKS_JSON_MULTIPLE_CHUNKS_1, Mockups.DATABRICKS_JSON_MULTIPLE_CHUNKS_2);

          var options = new SqlWarehouseOptions { Host = "https://adb-xxxx-1.azuredatabricks.net", ApiKey = "eyj..." };
            var connection = new SqlWarehouseConnection(options, httpClient);

            var command = new SqlWarehouseCommand(connection, "SELECT * FROM lineitem");
            var lstItems = await command.LoadAsync<LineItem>();

            Assert.NotNull(lstItems);
            Assert.IsType<List<LineItem>>(lstItems);
            Assert.Equal(6, lstItems.Count);

        }
    }

    // "SELECT l_orderkey, l_linenumber, l_quantity, l_returnflag, l_shipdate 
    public class LineItem
    {
        [JsonPropertyName("l_orderkey")]
        public long OrderKey { get; set; }
        [JsonPropertyName("l_linenumber")]
        public int LineNumber { get; set; }
        [JsonPropertyName("l_quantity")]
        public decimal Quantity { get; set; }
        [JsonPropertyName("l_returnflag")]
        public string ReturnFlag { get; set; }
        [JsonPropertyName("l_shipdate")]
        public DateOnly ShipDate { get; set; }
        [JsonPropertyName("l_distance")]
        public double Distance { get; set; }
        [JsonPropertyName("l_pickup_datetime")]
        public DateTime PickupDatetime { get; set; }
        [JsonPropertyName("l_isvalid")]
        public bool IsValid { get; set; }
    }

}
