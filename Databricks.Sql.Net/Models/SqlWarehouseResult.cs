using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseResult
    {
        [JsonPropertyName("row_count")]
        public int RowCount { get; init; }

        [JsonPropertyName("row_offset")]
        public int RowOffset { get; init; }

        [JsonPropertyName("chunk_index")]
        public int ChunkIndex { get; init; }

        [JsonPropertyName("next_chunk_index")]
        public int NextChunkIndex { get; init; }

        [JsonPropertyName("next_chunk_internal_link")]
        public string NextChunkInternalLink { get; init; }

        [JsonPropertyName("data_array")]
        public string[][] DataArray { get; init; }

        [JsonPropertyName("external_links")]
        public List<SqlWarehouseExternalLink> ExternalLinks { get; init; }

    }
}
