using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseChunk
    {
        [JsonPropertyName("chunk_index")]
        public int ChunkIndex { get; set; }
        [JsonPropertyName("row_count")]
        public int RowCount { get; set; }
        [JsonPropertyName("row_offset")]
        public int RowOffset { get; set; }
    }
}
