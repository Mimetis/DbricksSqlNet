using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseManifestSchema
    {
        [JsonPropertyName("column_count")]
        public int ColumnCount { get; init; }

        [JsonPropertyName("columns")]
        public SqlWarehouseManifestSchemaColumn[] Columns { get; init; }
    }
}
