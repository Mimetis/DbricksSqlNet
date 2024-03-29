using System.Text.Json.Serialization;

namespace DataBrickConnector.Models
{
    public record DbricksSqlManifest
    {
        [JsonPropertyName("format")]
        public string Format { get; init; }

        [JsonPropertyName("schema")]
        public DbricksManifestSchema Schema { get; init; }

        [JsonPropertyName("total_row_count")]
        public int TotalRowCount { get; init; }

        [JsonPropertyName("truncated")]
        public bool Truncated { get; init; }
    }
}
