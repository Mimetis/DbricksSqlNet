using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseSqlManifest
    {
        [JsonPropertyName("format")]
        public string Format { get; init; }

        [JsonPropertyName("schema")]
        public SqlWarehouseManifestSchema Schema { get; init; }

        [JsonPropertyName("total_row_count")]
        public int TotalRowCount { get; init; }

        [JsonPropertyName("truncated")]
        public bool Truncated { get; init; }
    }
}
