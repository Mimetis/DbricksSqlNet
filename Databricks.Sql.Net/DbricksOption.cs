using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net
{
    public class DbricksOptions
    {

        // ctor
        public DbricksOptions() { }

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

    }
}
