using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Databricks.Sql.Net.Models.Sql
{
    public record Statement
    {
        public string StatementId { get; init; }
        public StatementStatus Status { get; init; }
        public StatementSqlManifest Manifest { get; init; }
        public StatementResult Result { get; init; }
    }

    public record StatementResult
    {
        public int RowCount { get; init; }
        public int RowOffset { get; init; }
        public int ChunkIndex { get; init; }
        public int? NextChunkIndex { get; init; }
        public string NextChunkInternalLink { get; init; }
        public string[][] DataArray { get; set; }
        public List<StatementExternalLink> ExternalLinks { get; init; }

    }

    public record StatementSqlManifest
    {
        public string Format { get; init; }
        public StatementManifestSchema Schema { get; init; }
        public int TotalRowCount { get; init; }
        public int TotalChunkCount { get; init; }
        public bool Truncated { get; init; }
        public List<StatementChunk> Chunks { get; init; }
    }

    public record StatementStatus
    {
        public string State { get; init; }
        public StatementStatusError Error { get; init; }
    }


    public record StatementStatusError
    {
        public string ErrorCode { get; init; }
        public string Message { get; init; }
    }


    public record StatementParameters
    {
        public string WarehouseId { get; init; }
        public string WaitTimeout { get; init; }
        public string Catalog { get; init; }
        public string Schema { get; init; }
        public string Statement { get; init; }
    }


    public record StatementManifestSchema
    {
        public int ColumnCount { get; init; }
        public StatementManifestSchemaColumn[] Columns { get; init; }
    }


    public record StatementManifestSchemaColumn
    {
        public string Name { get; init; }
        public string TypeText { get; init; }
        public string TypeName { get; init; }
        public int Position { get; init; }
    }

    public record StatementExternalLink
    {

        public int ByteCount { get; set; }
        public int ChunkIndex { get; set; }
        public DateTime Expiration { get; set; }
        public string ExternalLink { get; set; }
        public int RowCount { get; set; }
        public int RowOffset { get; set; }
    }
    public record StatementChunk
    {
        public int ChunkIndex { get; set; }
        public int RowCount { get; set; }
        public int RowOffset { get; set; }
    }

    public class StatementProgress
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
        public List<StatementExternalLink> ExternalLinks { get; init; }
        public override string ToString() => 
            $"StatementId: {StatementId}, State: {State}, TotalRowCount: {TotalRowCount}, TotalChunkCount: {TotalChunkCount}, ChunkRowCount: {ChunkRowCount}, ChunkRowOffset: {ChunkRowOffset}, ChunkIndex: {ChunkIndex}, NextChunkIndex: {NextChunkIndex}, NextChunkInternalLink: {NextChunkInternalLink}";
    }
}
