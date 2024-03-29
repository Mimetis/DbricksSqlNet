using System.Text.Json.Serialization;

namespace DataBrickConnector.Models
{
    public record DbricksStatusError
    {
        [JsonPropertyName("error_code")]
        public string ErrorCode { get; init; }

        [JsonPropertyName("message")]
        public string Message { get; init; }
    }
}
