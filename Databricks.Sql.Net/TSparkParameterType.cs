using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net
{
    public static class TSparkParameterType
    {
        public static string VOID => "VOID"; // aka NULL
        public static string STRING => "STRING";
        public static string DATE => "DATE";
        public static string TIMESTAMP => "TIMESTAMP";
        public static string FLOAT => "FLOAT";
        public static string DECIMAL => "DECIMAL";
        public static string DOUBLE => "DOUBLE";
        public static string INTEGER => "INTEGER";
        public static string BIGINT => "BIGINT";
        public static string SMALLINT => "SMALLINT";
        public static string TINYINT => "TINYINT";
        public static string BOOLEAN => "BOOLEAN";
        public static string INTERVALMONTH => "INTERVAL MONTH";
        public static string INTERVALDAY => "INTERVAL DAY";
    }
}
