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


        [Fact]
        public void Check_Parameters_In_Body()
        {
            var httpClient = Mockups.GetHttpClientMock(Mockups.DATABRICKS_JSON_ARRAY_RESPONSE);

            var options = new SqlWarehouseOptions { Host = "https://adb-xxxx-1.azuredatabricks.net", ApiKey = "eyj..." };
            var connection = new SqlWarehouseConnection(options, httpClient);

            var command = new SqlWarehouseCommand(connection, "SELECT * FROM lineitem WHERE ...");
            command.Parameters.AddBigInt("v_bigint", 1);
            command.Parameters.AddBoolean("v_boolean", true);
            command.Parameters.AddDate("v_date", new DateOnly(2024, 02, 28));
            command.Parameters.AddDecimal("v_decimal_1", 2);
            command.Parameters.AddDecimal("v_decimal_2", (decimal)2.85, 6, 3);
            command.Parameters.AddDouble("v_double", 3);
            command.Parameters.AddFloat("v_float", (float)4.458);
            command.Parameters.AddInt("v_int", 5);
            command.Parameters.AddSmallInt("v_smallint", 6);
            command.Parameters.AddString("v_string", "test");
            command.Parameters.AddTimestamp("v_timestamp", new DateTime(2024, 02, 28, 5, 55, 15, 155, 100));
            command.Parameters.AddTimestampTz("v_timestamp_tz", new DateTime(2024, 02, 28, 5, 55, 15, 155, 100));
            command.Parameters.AddTinyint("v_tinyint", 5);
            command.Parameters.Add("v_custom", SqlWarehouseType.BOOLEAN, false);

            // Act
            var content = command.BuildRequestContent(3);
            var deserializedContent = JsonSerializer.Deserialize<JsonElement>(content);

            var parameters = deserializedContent.GetProperty("parameters");

            var v_bigint = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_bigint");
            Assert.Equal(1, v_bigint.GetProperty("value").GetInt64());
            Assert.Equal("BIGINT", v_bigint.GetProperty("type").GetString());

            // Check all parameters and make an assert in property value and an assert on type
            var v_boolean = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_boolean");
            Assert.True(v_boolean.GetProperty("value").GetBoolean());
            Assert.Equal("BOOLEAN", v_boolean.GetProperty("type").GetString());

            var v_date = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_date");
            Assert.Equal("2024-02-28", v_date.GetProperty("value").GetString());
            Assert.Equal("DATE", v_date.GetProperty("type").GetString());

            var v_decimal_1 = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_decimal_1");
            Assert.Equal(2, v_decimal_1.GetProperty("value").GetDecimal());
            Assert.Equal("DECIMAL", v_decimal_1.GetProperty("type").GetString());

            var v_decimal_2 = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_decimal_2");
            Assert.Equal((decimal)2.85, v_decimal_2.GetProperty("value").GetDecimal());
            Assert.Equal("DECIMAL(6,3)", v_decimal_2.GetProperty("type").GetString());

            var v_double = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_double");
            Assert.Equal(3, v_double.GetProperty("value").GetDouble());
            Assert.Equal("DOUBLE", v_double.GetProperty("type").GetString());

            var v_float = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_float");
            Assert.Equal((float)4.458, v_float.GetProperty("value").GetSingle());
            Assert.Equal("FLOAT", v_float.GetProperty("type").GetString());

            var v_int = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_int");
            Assert.Equal(5, v_int.GetProperty("value").GetInt32());
            Assert.Equal("INT", v_int.GetProperty("type").GetString());

            var v_smallint = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_smallint");
            Assert.Equal(6, v_smallint.GetProperty("value").GetInt16());
            Assert.Equal("SMALLINT", v_smallint.GetProperty("type").GetString());

            var v_string = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_string");
            Assert.Equal("test", v_string.GetProperty("value").GetString());
            Assert.Equal("STRING", v_string.GetProperty("type").GetString());

            var v_timestamp = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_timestamp");
            Assert.Equal("2024-02-28T05:55:15.1551", v_timestamp.GetProperty("value").GetString());
            Assert.Equal("TIMESTAMP", v_timestamp.GetProperty("type").GetString());

            var v_timestamp_tz = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_timestamp_tz");
            Assert.Equal("2024-02-28T05:55:15.1551", v_timestamp_tz.GetProperty("value").GetString());
            Assert.Equal("TIMESTAMP_NTZ", v_timestamp_tz.GetProperty("type").GetString());

            var v_tinyint = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_tinyint");
            Assert.Equal(5, v_tinyint.GetProperty("value").GetByte());
            Assert.Equal("TINYINT", v_tinyint.GetProperty("type").GetString());

            var v_custom = parameters.EnumerateArray().First(je => je.GetProperty("name").GetString() == "v_custom");
            Assert.False(v_custom.GetProperty("value").GetBoolean());
            Assert.Equal("BOOLEAN", v_custom.GetProperty("type").GetString());
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
