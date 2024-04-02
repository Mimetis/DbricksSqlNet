using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseManifestSchemaColumn
    {
        [JsonPropertyName("name")]
        public string Name { get; init; }

        [JsonPropertyName("type_text")]
        public string TypeText { get; init; }

        [JsonPropertyName("type_name")]
        public string TypeName { get; init; }

        [JsonPropertyName("position")]
        public int Position { get; init; }
    }
}