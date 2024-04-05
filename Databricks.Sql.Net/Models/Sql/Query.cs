using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Models.Sql
{

    public class Query
    {
        public int Count { get; set; }
        public int Page { get; set; }
        public int PageSize { get; set; }
        public List<QueryResult> Results { get; set; }
    }

    public class QueryResult
    {
        public string Id { get; set; }
        public string Parent { get; set; }
        public string LatestQueryDataId { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Query { get; set; }
        public string QueryHash { get; set; }
        public bool? IsArchived { get; set; }
        public bool? IsDraft { get; set; }
        public DateTime? UpdatedAt { get; set; }
        public DateTime? CreatedAt { get; set; }
        public string DataSourceId { get; set; }
        public QueryOptions Options { get; set; }
        public string[] Tags { get; set; }
        public bool? IsSafe { get; set; }
        public long? UserId { get; set; }
        public long? LastModifiedById { get; set; }
        public QueryVisualization[] Visualizations { get; set; }
        public bool? IsFavorite { get; set; }
        public QueryUser User { get; set; }
        public QueryUser LastModifiedBy { get; set; }
        public bool? CanEdit { get; set; }
        public string PermissionTier { get; set; }
        public string RunAsRole { get; set; }
    }

    public class QueryOptions
    {
        public DateTime? MovedToTrashAt { get; set; }
        public QueryParameter[] Parameters { get; set; }
    }

    public class QueryParameter
    {
        public string Title { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public string EnumOptions { get; set; }
        public string QueryId { get; set; }
        public QueryMultivaluesoptions MultiValuesOptions { get; set; }
        public object Value { get; set; }
    }

    public class QueryMultivaluesoptions
    {
        public string Separator { get; set; }
        public string Prefix { get; set; }
        public string Suffix { get; set; }
    }

    public class QueryUser
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
    }


    public class QueryVisualization
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public DateTime? UpdatedAt { get; set; }
        public DateTime? CreatedAt { get; set; }
    }



    public class QueryHistory
    {
        public string NextPageToken { get; set; }
        public bool? HasNextPage { get; set; }
        [JsonPropertyName("res")]
        public QueryHistoryResult[] Results { get; set; }
    }

    public class QueryHistoryResult
    {
        public string QueryId { get; set; }
        public string Status { get; set; }
        public string QueryText { get; set; }
        public long? QueryStartTimeMs { get; set; }
        public long? ExecutionEndTimeMs { get; set; }
        public long? QueryEndTimeMs { get; set; }
        public long? UserId { get; set; }
        public string UserName { get; set; }
        public string SparkUiUrl { get; set; }
        public string EndpointId { get; set; }
        public string WarehouseId { get; set; }
        public string LookupKey { get; set; }
        public string ErrorMessage { get; set; }
        public int? RowsProduced { get; set; }
        public bool? CanSubscribeToLiveQuery { get; set; }
        public Metrics Metrics { get; set; }
        public bool? IsFinal { get; set; }
        public ChannelUsed ChannelUsed { get; set; }
        public int? Duration { get; set; }
        public long? ExecutedAsUserId { get; set; }
        public string ExecutedAsUserName { get; set; }
        public string PlansState { get; set; }
        public string StatementType { get; set; }
    }

    public class Metrics
    {
        public int? TotalTimeMs { get; set; }
        public int? ReadBytes { get; set; }
        public int? RowsProducedCount { get; set; }
        public int? CompilationTimeMs { get; set; }
        public int? ExecutionTimeMs { get; set; }
        public int? ReadRemoteBytes { get; set; }
        public int? WriteRemoteBytes { get; set; }
        public int? ReadCacheBytes { get; set; }
        public int? SpillToDiskBytes { get; set; }
        public int? TaskTotalTimeMs { get; set; }
        public int? ReadFilesCount { get; set; }
        public int? ReadPartitionsCount { get; set; }
        public int? PhotonTotalTimeMs { get; set; }
        public int? RowsReadCount { get; set; }
        public int? ResultFetchTimeMs { get; set; }
        public int? NetworkSentBytes { get; set; }
        public bool? ResultFromCache { get; set; }
        public int? PrunedBytes { get; set; }
        public int? PrunedFilesCount { get; set; }
        public long? ProvisioningQueueStartTimestamp { get; set; }
        public long? OverloadingQueueStartTimestamp { get; set; }
        public long? QueryCompilationStartTimestamp { get; set; }
        public int? MetadataTimeMs { get; set; }
        public int? PlanningTimeMs { get; set; }
        public int? QueryExecutionTimeMs { get; set; }
    }

    public class ChannelUsed
    {
        public string Name { get; set; }
        public string DbsqlVersion { get; set; }
    }

}
