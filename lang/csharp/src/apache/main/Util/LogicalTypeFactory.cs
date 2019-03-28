using System.Collections.Generic;

namespace Avro.Util
{
    /// <summary>
    /// A factory for logical type implementations.
    /// </summary>
    public class LogicalTypeFactory
    {
        private readonly IDictionary<string, LogicalType> _logicalTypes;

        /// <summary>
        /// Returns the <see cref="LogicalTypeFactory" /> singleton.
        /// </summary>
        /// <returns>The <see cref="LogicalTypeFactory" /> singleton. </returns>
        public static LogicalTypeFactory Instance { get; } = new LogicalTypeFactory();

        private LogicalTypeFactory()
        {
            _logicalTypes = new Dictionary<string, LogicalType>()
            {
                { Date.LogicalTypeName, new Date() }
            };
        }

        /// <summary>
        /// Retrieves a logical type implementation for a given logical schema.
        /// </summary>
        /// <param name="schema">The schema.</param>
        /// <param name="ignoreInvalidOrUnknown">A flag to indicate if an exception should be thrown for invalid
        /// or unknown logical types.</param>
        /// <returns>A <see cref="LogicalType" />.</returns>
        public LogicalType GetFromLogicalSchema(LogicalSchema schema, bool ignoreInvalidOrUnknown = false)
        {
            try
            {
                if (!_logicalTypes.TryGetValue(schema.LogicalTypeName, out LogicalType logicalType))
                    throw new AvroTypeException("Logical type '" + schema.LogicalTypeName + "' is not supported.");

                logicalType.ValidateSchema(schema);

                return logicalType;
            }
            catch (AvroTypeException err)
            {
                if (!ignoreInvalidOrUnknown)
                    throw err;
            }

            return null;
        }
    }
}
