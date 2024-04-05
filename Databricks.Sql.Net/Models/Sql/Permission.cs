using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Models.Sql
{

    public class WarehousePermissions
    {
        public string ObjectId { get; set; }
        public string ObjectType { get; set; }
        public AccessControlList[] AccessControlList { get; set; }
    }

    public class AccessControlList
    {
        public string UserName { get; set; }
        public string GroupName { get; set; }
        public string ServicePrincipalName { get; set; }
        public string DisplayName { get; set; }
        public AllPermissions[] AllPermissions { get; set; }
    }

    public class AllPermissions
    {
        public string PermissionLevel { get; set; }
        public bool? Inherited { get; set; }
        public string[] InheritedFromObject { get; set; }
    }

}
