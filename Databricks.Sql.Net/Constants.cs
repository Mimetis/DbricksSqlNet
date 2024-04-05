using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net
{
    public class Constants
    {
        public const string DatabricksResourceId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";

        // statements
        public const string SqlStatementsPath = "/api/2.0/sql/statements";
        public const string SqlStatementsResultPath = "/api/2.0/sql/statements/{0}";

        // list of sql warehouses
        public const string SqlWarehousesList = "/api/2.0/preview/sql/data_sources";

        // queries
        public const string SqlQueriesList = "/api/2.0/preview/sql/queries";
        public const string SqlQueriesHistory = "/api/2.0/sql/history/queries";

        // permissions
        public const string SqlWarehousePermissions = "/api/2.0/permissions/warehouses/{0}";
    }
}
