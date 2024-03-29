using System.Text.Json.Serialization;

namespace DataBrickConnector.Models
{
    public record DbricksStatus
    {
        [JsonPropertyName("state")]
        public string State { get; init; }

        [JsonPropertyName("error")]
        public DbricksStatusError Error { get; init; }
    }
}
