using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseResult
    {
        [JsonPropertyName("row_count")]
        public int RowCount { get; init; }

        [JsonPropertyName("data_array")]
        public string[][] DataArray { get; init; }
    }
}
