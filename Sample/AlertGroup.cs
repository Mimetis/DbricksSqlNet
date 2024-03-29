using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Sample
{
    public class AlertGroup
    {
        [JsonPropertyName("Type")]
        public string Type { get; set; }
        [JsonPropertyName("StockStatus")]
        public string StockStatus { get; set; }
        [JsonPropertyName("Issue")]
        public string Issue { get; set; }

        [JsonPropertyName("IssueDate")]
        public DateOnly IssueDate { get; set; }
        [JsonPropertyName("EventTime")]
        public string EventTime { get; set; }
        [JsonPropertyName("MaxPriority")]
        public decimal MaxPriority { get; set; }
        [JsonPropertyName("ExpectedDeliveryDate")]
        public DateOnly ExpectedDeliveryDate { get; set; }
        [JsonPropertyName("Sku")]
        public string Sku { get; set; }
        [JsonPropertyName("SkuName")]
        public string SkuName { get; set; }
        [JsonPropertyName("LocationId")]
        public string LocationId { get; set; }
        [JsonPropertyName("LocationDesc")]
        public string LocationDesc { get; set; }
        [JsonPropertyName("CplId")]
        public string CplId { get; set; }
        [JsonPropertyName("CplName")]
        public string CplName { get; set; }
        [JsonPropertyName("VolumeSource")]
        public string VolumeSource { get; set; }
        [JsonPropertyName("TotalVolumeAtRisk")]
        public decimal TotalVolumeAtRisk { get; set; }
        [JsonPropertyName("TotalOrderVolume")]
        public decimal TotalOrderVolume { get; set; }
        [JsonPropertyName("TotalNsr")]
        public decimal TotalNsr { get; set; }
        [JsonPropertyName("TotalSlaIncurred")]
        public decimal TotalSlaIncurred { get; set; }
        [JsonPropertyName("TotalOpExCost")]
        public decimal TotalOpExCost { get; set; }
        [JsonPropertyName("TotalProfitLoss")]
        public decimal TotalProfitLoss { get; set; }
        [JsonPropertyName("AlertsCount")]
        public long AlertsCount { get; set; }

    }
}
