using System.Text.Json.Serialization;

namespace DataBrickConnector.Models
{
    public record DbricksManifestSchema
    {
        [JsonPropertyName("column_count")]
        public int ColumnCount { get; init; }

        [JsonPropertyName("columns")]
        public DbricksManifestSchemaColumn[] Columns { get; init; }
    }
}
