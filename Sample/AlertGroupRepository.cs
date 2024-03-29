using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using DataBrickConnector.DataBrickConnector;
using DataBrickConnector.Models;

namespace Sample
{
    public class AlertGroupRepository
    {
        public AlertGroupRepository()
        {
        }

        public static String GetAlertGroupsString(DateOnly from, DateOnly to)
        {

            var statement = new StringBuilder(@$"
                    SELECT a.Type,
                    a.StockStatus,
                    a.Issue,
                    a.IssueDate,
                    a.EventTime,
                    Max(a.AlertPriority) as MaxPrioriry,
                    a.ExpectedDeliveryDate,
                    a.ProductId as Sku,
                    a.ProductName as SkuName,
                    loc.plant as LocationId,
                    loc.plant_desc as LocationDesc,
                    a.CustomerPlanningLevel as CplId,
                    a.CustomerPlanningLevelName as CplName,
                    a.VolumeSource,
                    SUM(a.VolumeAtRisk) as TotalVolumeAtRisk,
                    SUM(a.CPLOrderForecastVolume) as TotalOrderVolume,
                    SUM(a.Nsr) as TotalNsr,
                    SUM(a.SlaIncurred) as TotalSlaIncurred,
                    SUM(a.OpExCost ) as TotalOpExCost,
                    SUM(a.ProfitLoss ) as TotalProfitLoss,
                    count(*) as AlertsCount
                    FROM inc_data_model.alerts_1_2_ui_v1 as a
                    JOIN inc_data_model.locations_ui_v1 as loc on a.LocationId = loc.plant
                    WHERE ((a.Type =1 AND isnotnull(a.Issue)  AND a.horizon<=a.WorkingHorizon and a.VolumeAtRisk > 0) 
                            OR (a.Type = 2 AND a.VolumeAtRisk > 0 AND (a.VolumeSource = 'Forecasts' OR a.VolumeSource = 'Orders')))
                    AND a.ExpectedDeliveryDate >= '{from.ToString("yyyy-MM-dd")}' AND a.ExpectedDeliveryDate <= '{to.ToString("yyyy-MM-dd")}'");

            statement.AppendLine(@$"
                    GROUP BY a.Type,a.Issue, a.IssueDate, a.EventTime, a.StockStatus, a.VolumeSource, a.CustomerPlanningLevel, a.CustomerPlanningLevelName,
                    loc.plant, loc.plant_desc, a.ExpectedDeliveryDate, a.LocationId, a.ProductId, a.ProductName
                    ORDER BY a.Type asc, MaxPrioriry desc;");

            return statement.ToString();
        }

        public static String GetAlertGroupsStringWithParameters()
        {

            var statement = new StringBuilder(@$"
                    SELECT a.Type,
                    a.StockStatus,
                    a.Issue,
                    a.IssueDate,
                    a.EventTime,
                    Max(a.AlertPriority) as MaxPrioriry,
                    a.ExpectedDeliveryDate,
                    a.ProductId as Sku,
                    a.ProductName as SkuName,
                    loc.plant as LocationId,
                    loc.plant_desc as LocationDesc,
                    a.CustomerPlanningLevel as CplId,
                    a.CustomerPlanningLevelName as CplName,
                    a.VolumeSource,
                    SUM(a.VolumeAtRisk) as TotalVolumeAtRisk,
                    SUM(a.CPLOrderForecastVolume) as TotalOrderVolume,
                    SUM(a.Nsr) as TotalNsr,
                    SUM(a.SlaIncurred) as TotalSlaIncurred,
                    SUM(a.OpExCost ) as TotalOpExCost,
                    SUM(a.ProfitLoss ) as TotalProfitLoss,
                    count(*) as AlertsCount
                    FROM inc_data_model.alerts_1_2_ui_v1 as a
                    JOIN inc_data_model.locations_ui_v1 as loc on a.LocationId = loc.plant
                    WHERE ((a.Type =1 AND isnotnull(a.Issue)  AND a.horizon<=a.WorkingHorizon and a.VolumeAtRisk > 0) 
                            OR (a.Type = 2 AND a.VolumeAtRisk > 0 AND (a.VolumeSource = 'Forecasts' OR a.VolumeSource = 'Orders')))
                    AND a.ExpectedDeliveryDate >= :from AND a.ExpectedDeliveryDate <= :to");

            statement.AppendLine(@$"
                    GROUP BY a.Type,a.Issue, a.IssueDate, a.EventTime, a.StockStatus, a.VolumeSource, a.CustomerPlanningLevel, a.CustomerPlanningLevelName,
                    loc.plant, loc.plant_desc, a.ExpectedDeliveryDate, a.LocationId, a.ProductId, a.ProductName
                    ORDER BY a.Type asc, MaxPrioriry desc;");

            return statement.ToString();
        }

    }
}
