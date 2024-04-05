using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Models.Sql
{
    public class Schema
    {
        public string Name { get; set; }
        public string Catalog { get; set; }
        public string Comment { get; set; }
        public string Location { get; set; }

        public List<Table> Tables { get; set; }
    }

    public class Table
    {
        public string Name { get; set; }
        public string Schema { get; set; }
        public string Catalog { get; set; }
        public bool IsTemporary { get; set; }
        public List<Column> Columns { get; set; }

        public Dictionary<string, string> Properties { get; set; }

    }

    public class Column
    {
        public string Name { get; set; }
        public string DataType{ get; set; }
    }
}
