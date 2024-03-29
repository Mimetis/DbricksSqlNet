using System.Text.Json.Serialization;

namespace DataBrickConnector.Models
{
    public record DbricksParameters
    {
        [JsonPropertyName("warehouse_id")]
        public string WarehouseId { get; init; }

        [JsonPropertyName("wait_timeout")]
        public string WaitTimeout { get; init; }

        [JsonPropertyName("catalog")]
        public string Catalog { get; init; }

        [JsonPropertyName("schema")]
        public string Schema { get; init; }

        [JsonPropertyName("statement")]
        public string Statement { get; init; }
    }
}