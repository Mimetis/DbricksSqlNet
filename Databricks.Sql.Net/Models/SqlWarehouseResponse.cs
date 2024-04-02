using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseResponse
    {
        [JsonPropertyName("statement_id")]
        public string StatementId { get; init; }

        [JsonPropertyName("status")]
        public SqlWarehouseStatus Status { get; init; }

        [JsonPropertyName("manifest")]
        public SqlWarehouseSqlManifest Manifest { get; init; }

        [JsonPropertyName("result")]
        public SqlWarehouseResult Result { get; init; }
    }
}
