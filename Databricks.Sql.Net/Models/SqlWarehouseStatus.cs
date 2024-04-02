using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseStatus
    {
        [JsonPropertyName("state")]
        public string State { get; init; }

        [JsonPropertyName("error")]
        public SqlWarehouseStatusError Error { get; init; }
    }
}
