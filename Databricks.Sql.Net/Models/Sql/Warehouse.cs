using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Models.Sql
{
    public class Warehouse
    {

        public string Id { get; init; }
        public string Name { get; init; }
        public string Type { get; init; }
        public string WarehouseId { get; init; }
        public string Syntax { get; init; }
        public int Paused { get; init; }
        public string PausReason { get; init; }
        public bool SupportsAutoLimit { get; init; }
        public bool ViewOnly { get; init; }
    }
}
