using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Options
{
    public class SqlWarehouseOptions
    {

        // ctor
        public SqlWarehouseOptions() { }

        /// <summary>
        /// Databricks Host
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// Databricks Api Key
        /// </summary>
        public string ApiKey { get; set; }

        /// <summary>
        /// Databricks Warehouse Id
        /// </summary>
        public string WarehouseId { get; set; }

        /// <summary>
        /// Databricks Catalog
        /// </summary>
        public string Catalog { get; set; }

        /// <summary>
        /// Databricks Schema
        /// </summary>
        public string Schema { get; set; }

        /// <summary>
        /// Gets or Sets the wait timeout in seconds (default 30)
        /// </summary>
        public int WaitTimeout { get; set; } = 30;

        /// <summary>
        /// Get or Sets the managed identity client id. Only used when UseManagedIdentity is true
        /// </summary>
        public string ManagedIdentityClientId { get; set; }

        /// <summary>
        /// Get or Sets the tenant id
        /// </summary>
        public string TenantId { get; set; }

    }
}
