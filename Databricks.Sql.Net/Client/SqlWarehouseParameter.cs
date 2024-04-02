using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Client
{
    public class SqlWarehouseParameter
    {
        /// <summary>
        /// Gets or Sets the parameter name.
        /// </summary>
        public string ParameterName { get; set; }

        /// <summary>
        /// Gets or Sets the parameter type.
        /// </summary>
        public SqlWarehouseType Type { get; set; }

        /// <summary>
        /// Gets the parameter full type name with size and scale.
        /// </summary>
        public string TypeName
        {
            get
            {
                string stringValue = Enum.GetName(typeof(SqlWarehouseType), Type);

                var typeNameSb = new StringBuilder(stringValue);

                if (Size > 0 && (Type == SqlWarehouseType.DOUBLE || Type == SqlWarehouseType.DECIMAL || Type == SqlWarehouseType.FLOAT))
                {
                    typeNameSb.Append($"({Size}");

                    if (Scale > 0)
                        typeNameSb.Append($",{Scale}");

                    typeNameSb.Append(')');
                }

                return typeNameSb.ToString();
            }
        }

        /// <summary>
        /// Gets or Sets the column precision.
        /// </summary>
        public virtual byte Precision { get; set; }

        /// <summary>
        /// Gets or Sets the column scale.
        /// </summary>
        public virtual byte Scale { get; set; }

        /// <summary>
        /// Gets or Sets the column size.
        /// </summary>
        public virtual byte Size { get; set; }

        /// <summary>
        /// Gets or Sets the value of the parameter.
        /// </summary>
        public object Value { get; set; }
    }
}
