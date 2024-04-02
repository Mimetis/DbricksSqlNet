using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseStatusError
    {
        [JsonPropertyName("error_code")]
        public string ErrorCode { get; init; }

        [JsonPropertyName("message")]
        public string Message { get; init; }
    }
}
