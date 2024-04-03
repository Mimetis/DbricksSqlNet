using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Models
{
    public class SqlWarehouseProgress
    {
        public string StatementId { get; init; }
        public string State { get; init; }

        public int? TotalRowCount { get; init; }

        public int? TotalChunkCount { get; init; }

        public int? ChunkRowCount { get; init; }

        public int? ChunkRowOffset { get; init; }

        public int? ChunkIndex { get; init; }

        public int? NextChunkIndex { get; init; }

        public string NextChunkInternalLink { get; init; }

        public List<SqlWarehouseExternalLink> ExternalLinks { get; init; }

        public override string ToString()
        {
            return $"StatementId: {StatementId}, State: {State}, TotalRowCount: {TotalRowCount}, TotalChunkCount: {TotalChunkCount}, ChunkRowCount: {ChunkRowCount}, ChunkRowOffset: {ChunkRowOffset}, ChunkIndex: {ChunkIndex}, NextChunkIndex: {NextChunkIndex}, NextChunkInternalLink: {NextChunkInternalLink}";
        }
    }
}
