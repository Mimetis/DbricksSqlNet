using System.Text.Json.Serialization;

namespace DataBrickConnector.Models
{
    public record DbricksResult
    {
        [JsonPropertyName("row_count")]
        public int RowCount { get; init; }

        [JsonPropertyName("data_array")]
        public string[][] DataArray { get; init; }
    }
}
