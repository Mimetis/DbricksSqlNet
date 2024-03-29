using System.Text.Json.Serialization;

namespace DataBrickConnector.Models
{
    public record DbricksResponse
    {
        [JsonPropertyName("statement_id")]
        public string StatementId { get; init; }

        [JsonPropertyName("status")]
        public DbricksStatus Status { get; init; }

        [JsonPropertyName("manifest")]
        public DbricksSqlManifest Manifest { get; init; }

        [JsonPropertyName("result")]
        public DbricksResult Result { get; init; }
    }
}
