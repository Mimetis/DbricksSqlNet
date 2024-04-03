using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Models
{
    public record SqlWarehouseExternalLink
    {

        [JsonPropertyName("byte_count")]
        public int ByteCount { get; set; }
        [JsonPropertyName("chunk_index")]
        public int ChunkIndex { get; set; }
        [JsonPropertyName("expiration")]
        public DateTime Expiration { get; set; }
        [JsonPropertyName("external_link")]
        public string ExternalLink { get; set; }
        [JsonPropertyName("row_count")]
        public int RowCount { get; set; }
        [JsonPropertyName("row_offset")]
        public int RowOffset { get; set; }
    }
}
