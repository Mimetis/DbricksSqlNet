using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Enumeration
{
    public enum DbricksType
    {
        VOID, // aka NULL
        STRING,
        DATE,
        TIMESTAMP,
        FLOAT,
        DECIMAL,
        DOUBLE,
        INTEGER,
        BIGINT,
        BINARY,
        SMALLINT,
        TINYINT,
        BOOLEAN,
    }
}
