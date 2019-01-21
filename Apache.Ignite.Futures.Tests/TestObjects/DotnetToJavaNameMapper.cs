using Apache.Ignite.Core.Binary;
using System;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    /// <summary>
    /// Convert .NET to Java naming conventions. 
    /// This allows .NET types used as Ignite service parameters and results to have properties and namespaces 
    /// named in .NET style.
    /// </summary>
    class DotnetToJavaNameMapper : IBinaryNameMapper
    {
        /// <summary>
        /// Convert .NET property name to Java naming style by converting first letter to lower case.
        /// </summary>
        public string GetFieldName(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return char.ToLowerInvariant(name[0]) + name.Substring(1);
        }

        /// <summary>
        /// Convert .NET fully qualified type name to Java style by converting namespace to lower case.
        /// </summary>
        public string GetTypeName(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            // Remove assembly info and convert full name. Generics are not supported.
            var res = name;
            var i = res.IndexOf(',');
            if (i > 0)
                res = res.Substring(0, i);

            i = res.LastIndexOf('.');
            if (i > 0)
                res = res.Substring(0, i).ToLowerInvariant() + res.Substring(i);

            return res;
        }
    }
}
