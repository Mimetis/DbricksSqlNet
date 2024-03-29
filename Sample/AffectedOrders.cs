using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Sample
{
    public class AffectedOrders
    {
        [JsonPropertyName("Sku")]
        public string Sku { get; set; }
        [JsonPropertyName("SkuName")]
        public string SkuName { get; set; }
        [JsonPropertyName("LocationId")]
        public string LocationId { get; set; }
        [JsonPropertyName("LocationDesc")]
        public string LocationDesc { get; set; }
        [JsonPropertyName("ExpectedDeliveryDate")]
        public DateOnly ExpectedDeliveryDate { get; set; }
        [JsonPropertyName("CplId")]
        public string CplId { get; set; }
        [JsonPropertyName("CustomerId")]
        public string CustomerId { get; set; }
        [JsonPropertyName("CplName")]
        public string CplName { get; set; }
        [JsonPropertyName("VolumeAtRisk")]
        public decimal? VolumeAtRisk { get; set; }
        [JsonPropertyName("VolumeOrder")]
        public decimal? VolumeOrder { get; set; }
        [JsonPropertyName("VolumeSource")]
        public string? VolumeSource { get; set; }
        [JsonPropertyName("Nsr")]
        public decimal? Nsr { get; set; }
        [JsonPropertyName("OpExCost")]
        public decimal? OpExCost { get; set; }
        [JsonPropertyName("SlaIncurred")]
        public decimal? SlaIncurred { get; set; }
        [JsonPropertyName("ProfitLoss")]
        public decimal? ProfitLoss { get; set; }

        [JsonPropertyName("OrdersCount")]
        public long OrdersCount { get; set; }
        [JsonPropertyName("OrdersVolumeOrder")]
        public decimal? OrdersVolumeOrder { get; set; }
        [JsonPropertyName("OrdersVolumeAtRisk")]
        public decimal? OrdersVolumeAtRisk { get; set; }
        [JsonPropertyName("OrdersSlaIncurred")]
        public decimal? OrdersSlaIncurred { get; set; }
        [JsonPropertyName("OrdersProfitLoss")]
        public decimal? OrdersProfitLoss { get; set; }

        [JsonPropertyName("OrdersNsr")]
        public decimal? OrdersNsr { get; set; }
        public static string GetAffectedOrdersStatement()
        {
            var statement = @$"With OrdersSum as (
                                SELECT o.OrderId,
                                        o.CustomerId,
                                        Sum(o.OrderVolume) as OrdersVolumeOrder,
                                        Sum(o.VolumeAtRisk) as OrdersVolumeAtRisk,
                                        Sum(o.ProfitLoss) as OrdersProfitLoss,
                                        Sum(o.SlaIncurred) as OrdersSlaIncurred,
                                        Sum(o.nsr) as OrdersNsr
                                        From inc_data_model.orders_ui_v1 as o
                                        Group By o.OrderId, o.CustomerId
                                )
                                SELECT 
                                    a.ExpectedDeliveryDate, 
                                    a.LocationId,
                                    loc.plant_desc as LocationDesc, 
                                    a.ProductId as Sku, 
                                    a.ProductName as SkuName, 
                                    a.CustomerPlanningLevel as CplId, 
                                    a.CustomerPlanningLevelName as CplName,
                                    a.CustomerId,
                                    Sum(a.VolumeAtRisk) as VolumeAtRisk,
                                    Sum(a.OrderVolume) as VolumeOrder,
                                    a.VolumeSource,
                                    Sum(a.Nsr) as Nsr,
                                    Sum(a.OpExCost) as OpExCost,
                                    Sum(a.SlaIncurred) as SlaIncurred,
                                    Sum(a.ProfitLoss) as ProfitLoss,
                                    Count(DISTINCT a.OrderId) as OrdersCount,
                                    Sum(o.OrdersVolumeOrder) as OrdersVolumeOrder,
                                    Sum(o.OrdersVolumeAtRisk) as OrdersVolumeAtRisk,
                                    Sum(o.OrdersProfitLoss) as OrdersProfitLoss,
                                    Sum(o.OrdersSlaIncurred) as OrdersSlaIncurred,
                                    Sum(o.OrdersNsr) as OrdersNsr
                                FROM inc_data_model.alerts_1_2_ui_v1  as a
                                JOIN inc_data_model.locations_ui_v1 as loc on a.LocationId = loc.plant
                                JOIN OrdersSum as o on a.OrderId = o.OrderId and o.CustomerId = a.CustomerId
                                WHERE a.ExpectedDeliveryDate = :exp
                                AND a.LocationId = :loc
                                AND a.ProductId = :sku
                                AND a.VolumeSource = 'Orders'
                                AND a.VolumeAtRisk > 0
                                AND (a.Type = '1' OR a.Type = '2')
                                GROUP BY a.ExpectedDeliveryDate, 
                                a.LocationId,
                                loc.plant_desc,
                                a.ProductId, 
                                a.ProductName,
                                a.CustomerPlanningLevel, 
                                a.CustomerPlanningLevelName,
                                a.CustomerId,
                                a.VolumeSource";

            return statement;
        }
    }
}
